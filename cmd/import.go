package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pocketbase/pocketbase/core"
	"github.com/spf13/cobra"
)

const (
	maxLineSize = 10 * 1024 * 1024 // 10MB，单行最大大小
)

// ImportOptions 导入选项配置
type ImportOptions struct {
	UniqueKeys []string // 唯一键字段名列表，按优先级依次查找
	UpsertMode bool     // 是否启用upsert模式
	SkipUpdate bool     // 是否跳过已有记录的更新
	BatchSize  int      // 每批保存的记录数
}

// NewImportCommand 创建导入命令
func NewImportCommand(app core.App) *cobra.Command {
	var (
		batchSize  int
		uniqueKeys string
		upsertMode bool
		skipUpdate bool
	)

	cmd := &cobra.Command{
		Use:   "import [json文件路径] [集合名称]",
		Short: "导入JSON数据到指定集合",
		Long: `从JSON文件导入数据到指定的集合中。支持以下格式：
1. 标准JSON数组格式
2. 格式化的JSON（支持多行）
3. 每行一个JSON对象

如果未指定集合名称，将从JSON文件名中自动提取集合名称（支持以下格式）：
- xxx_export_2024-01-01.json -> xxx
- xxx.json -> xxx

重复数据处理选项：
- --unique-key (-k): 指定唯一键字段，用于判断重复记录（支持多个，用逗号分隔，优先使用第一个存在的字段）
- --upsert (-u): 启用upsert模式，存在则更新，不存在则新增
- --skip-update (-s): 跳过已有记录的更新（仅新增）`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("缺少JSON文件路径参数")
			}
			if len(args) > 2 {
				return fmt.Errorf("参数过多，最多接受2个参数：JSON文件路径和可选的集合名称")
			}
			if upsertMode && uniqueKeys == "" {
				return fmt.Errorf("启用upsert模式时，必须指定唯一键字段（--unique-key）")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			jsonFile := args[0]
			collectionName := ""
			if len(args) >= 2 {
				collectionName = args[1]
			}
			if collectionName == "" {
				collectionName = extractCollectionName(jsonFile)
				if collectionName == "" {
					return fmt.Errorf("无法从文件路径 %q 提取集合名称，请手动指定集合名称", jsonFile)
				}
				fmt.Printf("自动从文件名提取集合名称: %s\n", collectionName)
			}

			uniqueKeyList := strings.Split(uniqueKeys, ",")
			for i, k := range uniqueKeyList {
				uniqueKeyList[i] = strings.TrimSpace(k)
			}

			importOptions := ImportOptions{
				UniqueKeys: uniqueKeyList,
				UpsertMode: upsertMode,
				SkipUpdate: skipUpdate,
				BatchSize:  batchSize,
			}
			return importData(app, jsonFile, collectionName, importOptions)
		},
	}
	cmd.Flags().IntVarP(&batchSize, "batch-size", "b", 5000, "每批保存的记录数，默认5000")
	cmd.Flags().StringVarP(&uniqueKeys, "unique-key", "k", "", "唯一键字段名，用于判断重复记录（支持多个，用逗号分隔，如：id,username,email）")
	cmd.Flags().BoolVarP(&upsertMode, "upsert", "u", false, "启用upsert模式：存在则更新，不存在则新增")
	cmd.Flags().BoolVarP(&skipUpdate, "skip-update", "s", false, "跳过已有记录的更新（仅新增记录）")
	return cmd
}

// extractCollectionName 从JSON文件路径中提取集合名称
// 支持格式：xxx_export_2024-01-01.json -> xxx，xxx.json -> xxx
// jsonFile: JSON文件的完整路径或文件名
// 返回: 提取的集合名称，如果无法提取则返回空字符串
func extractCollectionName(jsonFile string) string {
	baseName := filepath.Base(jsonFile)
	extWithoutExt := strings.TrimSuffix(baseName, filepath.Ext(baseName))
	if extWithoutExt == "" {
		return ""
	}
	parts := strings.Split(extWithoutExt, "_export_")
	if len(parts) > 0 && parts[0] != "" {
		return parts[0]
	}
	return extWithoutExt
}

// importData 处理数据导入的主流程，支持自定义 batchSize
func importData(app core.App, jsonFile, collectionName string, opts ImportOptions) error {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 5000
	}

	// 获取目标集合
	collection, err := app.FindCollectionByNameOrId(collectionName)
	if err != nil {
		return fmt.Errorf("找不到集合 %s: %v", collectionName, err)
	}

	// 如果启用 upsert 或 skipUpdate 模式，预加载已存在的记录
	existingRecords := make(map[string]*core.Record)
	if (opts.UpsertMode || opts.SkipUpdate) && len(opts.UniqueKeys) > 0 {
		fmt.Printf("正在预加载已存在记录（唯一键：%v）...\n", opts.UniqueKeys)
		existingRecords, err = preloadExistingRecords(app, collection, opts.UniqueKeys)
		if err != nil {
			return fmt.Errorf("预加载已存在记录失败: %v", err)
		}
		fmt.Printf("已加载 %d 条已存在记录\n", len(existingRecords))
	}

	file, err := os.Open(jsonFile)
	if err != nil {
		return fmt.Errorf("打开文件失败: %v", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		b, err := reader.Peek(1)
		if err != nil {
			return fmt.Errorf("读取文件失败: %v", err)
		}
		if b[0] == ' ' || b[0] == '\n' || b[0] == '\r' || b[0] == '\t' {
			_, _ = reader.ReadByte()
			continue
		}
		if b[0] == '[' {
			return importJSONArray(app, reader, collection, opts, existingRecords)
		} else {
			return importJSONLines(app, reader, collection, opts, existingRecords)
		}
	}
}

// preloadExistingRecords 批量预加载已存在的记录
// 根据唯一键字段列表查询所有已存在的记录，构建多个 map 以便快速查找
func preloadExistingRecords(app core.App, collection *core.Collection, uniqueKeys []string) (map[string]*core.Record, error) {
	result := make(map[string]*core.Record)

	page := 1
	pageSize := 500
	for {
		records, err := app.FindRecordsByFilter(
			collection,
			"1=1",
			"-created",
			pageSize,
			(page-1)*pageSize,
		)
		if err != nil {
			return nil, err
		}

		if len(records) == 0 {
			break
		}

		for _, record := range records {
			// 尝试每个唯一键
			for _, uniqueKey := range uniqueKeys {
				keyValue := record.GetString(uniqueKey)
				if keyValue != "" {
					result[keyValue] = record
					break // 只存第一个匹配到的键值
				}
			}
		}

		if len(records) < pageSize {
			break
		}
		page++
	}

	return result, nil
}

// importJSONArray 流式导入标准JSON数组
func importJSONArray(app core.App, reader *bufio.Reader, collection *core.Collection, opts ImportOptions, existingRecords map[string]*core.Record) error {
	dec := json.NewDecoder(reader)
	unknownFields := make(map[string]struct{})
	t, err := dec.Token()
	if err != nil {
		return fmt.Errorf("读取JSON文件头失败: %v", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("JSON文件不是以数组开头: %v", t)
	}

	recordGenerator := func() (*core.Record, bool, error) {
		if !dec.More() {
			return nil, true, nil
		}
		var item map[string]any
		if err := dec.Decode(&item); err != nil {
			return nil, false, fmt.Errorf("解析JSON对象失败: %v", err)
		}
		record := mapToRecord(item, collection, func(field string) {
			if _, exists := unknownFields[field]; exists {
				return
			}
			unknownFields[field] = struct{}{}
		})
		return record, false, nil
	}

	if err := processBatchInsert(app, collection, opts, existingRecords, recordGenerator); err != nil {
		return err
	}

	if len(unknownFields) > 0 {
		fields := make([]string, 0, len(unknownFields))
		for f := range unknownFields {
			fields = append(fields, f)
		}
		fmt.Printf("警告: 导入字段在集合中不存在，collection=%s, fields=%s\n", collection.Name, strings.Join(fields, ","))
	}

	return nil
}

// importJSONLines 流式导入每行一个JSON对象
func importJSONLines(app core.App, reader *bufio.Reader, collection *core.Collection, opts ImportOptions, existingRecords map[string]*core.Record) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, maxLineSize), maxLineSize)
	lineNum := 0
	unknownFields := make(map[string]struct{})
	recordGenerator := func() (*core.Record, bool, error) {
		for scanner.Scan() {
			lineNum++
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			if len(line) > maxLineSize {
				fmt.Printf("警告: 第%d行数据过长，已跳过\n", lineNum)
				continue
			}
			var item map[string]any
			if err := json.Unmarshal([]byte(line), &item); err != nil {
				fmt.Printf("第%d行解析失败: %v，已跳过\n", lineNum, err)
				continue
			}
			record := mapToRecord(item, collection, func(field string) {
				if _, exists := unknownFields[field]; exists {
					return
				}
				unknownFields[field] = struct{}{}
			})
			return record, false, nil
		}
		if err := scanner.Err(); err != nil {
			return nil, true, fmt.Errorf("文件读取错误: %v", err)
		}
		return nil, true, nil
	}

	if err := processBatchInsert(app, collection, opts, existingRecords, recordGenerator); err != nil {
		return err
	}

	if len(unknownFields) > 0 {
		fields := make([]string, 0, len(unknownFields))
		for f := range unknownFields {
			fields = append(fields, f)
		}
		fmt.Printf("警告: 导入字段在集合中不存在，collection=%s, fields=%s\n", collection.Name, strings.Join(fields, ","))
	}

	return nil
}

// processBatchInsert 通用批量插入逻辑，支持 upsert 模式
// recordGenerator: 每次调用生成一个 *core.Record 和 bool（是否结束）
func processBatchInsert(app core.App, collection *core.Collection, opts ImportOptions, existingRecords map[string]*core.Record, recordGenerator func() (*core.Record, bool, error)) error {
	records := make([]*core.Record, 0, opts.BatchSize)
	totalCount := 0
	newCount := 0
	updateCount := 0
	skipCount := 0
	batch := 0
	startTime := time.Now()

	for {
		record, done, err := recordGenerator()
		if err != nil {
			return err
		}
		if done {
			break
		}
		if record == nil {
			continue
		}

		// Upsert 模式处理
		if (opts.UpsertMode || opts.SkipUpdate) && len(opts.UniqueKeys) > 0 {
			// 按优先级依次尝试每个唯一键
			var keyValue string
			for _, uniqueKey := range opts.UniqueKeys {
				keyValue = record.GetString(uniqueKey)
				if keyValue != "" {
					break
				}
			}

			if keyValue == "" {
				fmt.Printf("警告: 记录缺少所有唯一键字段 %v，已跳过。记录详情: %v\n", opts.UniqueKeys, record)
				skipCount++
				continue
			}

			existingRecord, exists := existingRecords[keyValue]
			if exists {
				// 记录已存在
				if opts.SkipUpdate {
					// 跳过更新模式，直接跳过
					skipCount++
					continue
				}

				// 检查是否需要更新（根据 updated 时间戳判断）
				if shouldUpdate(existingRecord, record) {
					// 直接在 record 上设置 ID 并标记为非新
					record.Id = existingRecord.Id
					record.MarkAsNotNew()

					records = append(records, record)
					updateCount++
				} else {
					skipCount++
				}
				continue
			} else {
				// 记录不存在，新增
				records = append(records, record)
				existingRecords[keyValue] = record // 更新内存中的记录
				newCount++
			}
		} else {
			// 普通模式，直接新增
			records = append(records, record)
			newCount++
		}

		totalCount++
		if len(records) >= opts.BatchSize {
			batch++
			savedCount, err := saveRecordsBatch(app, records, batch, totalCount)
			if err != nil {
				return err
			}
			newCount += savedCount - newCount
			records = make([]*core.Record, 0, opts.BatchSize)
		}
	}

	if len(records) > 0 {
		batch++
		if _, err := saveRecordsBatch(app, records, batch, totalCount); err != nil {
			return err
		}
	}

	totalTime := time.Since(startTime)
	if opts.UpsertMode {
		fmt.Printf("\n导入完成！总记录数: %d, 新增: %d, 更新: %d, 跳过: %d, 总用时: %.3f秒\n",
			totalCount, newCount, updateCount, skipCount, totalTime.Seconds())
	} else {
		if totalCount > 0 && totalTime.Seconds() > 0 {
			avgSpeed := float64(totalCount) / totalTime.Seconds()
			fmt.Printf("\n导入完成！总记录数: %d, 总用时: %.3f秒, 平均: %.3f条/秒\n",
				totalCount, totalTime.Seconds(), avgSpeed)
		} else {
			fmt.Printf("\n导入完成！总记录数: %d, 总用时: %.3f秒, 平均: -\n",
				totalCount, totalTime.Seconds())
		}
	}
	return nil
}

// shouldUpdate 判断是否应该更新已存在的记录
// 根据 updated 时间戳判断：新数据的 updated 时间大于已有记录时才更新
func shouldUpdate(existingRecord, newRecord *core.Record) bool {
	existingUpdated := existingRecord.GetDateTime("updated")
	newUpdated := newRecord.GetDateTime("updated")

	// 如果新数据的 updated 时间晚于已有记录，则更新
	if newUpdated.IsZero() || existingUpdated.IsZero() {
		// 如果无法获取时间戳，默认更新
		return true
	}

	return newUpdated.After(existingUpdated)
}

// saveRecordsBatch 统一批量保存逻辑，增强日志和进度
// 返回保存的记录数量
func saveRecordsBatch(app core.App, records []*core.Record, batchNum, totalCount int) (int, error) {
	err := app.RunInTransaction(func(txApp core.App) error {
		for i, record := range records {
			if err := txApp.Save(record); err != nil {
				recordJSON, _ := record.MarshalJSON()
				return fmt.Errorf("保存第%d批第%d条记录失败: %v\n记录内容:\n%s", batchNum, i+1, err, recordJSON)
			}
		}
		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("批量保存失败: %v", err)
	}

	fmt.Printf("成功导入第%d批数据，共%d条记录，累计导入%d条\n", batchNum, len(records), totalCount)
	return len(records), nil
}

// mapToRecord 辅助函数：map转Record，处理created/updated
// item: 原始数据map
// collection: 目标集合
// 返回: *core.Record
func mapToRecord(item map[string]any, collection *core.Collection, onUnknownField func(field string)) *core.Record {
	record := core.NewRecord(collection)

	knownFields := make(map[string]struct{}, len(collection.Fields)+3)
	for _, f := range collection.Fields {
		knownFields[f.GetName()] = struct{}{}
	}
	knownFields["id"] = struct{}{}
	knownFields["created"] = struct{}{}
	knownFields["updated"] = struct{}{}

	for key, value := range item {
		if key == "created" || key == "updated" {
			record.SetRaw(key, value)
		} else {
			record.Set(key, value)
		}

		if _, ok := knownFields[key]; !ok && onUnknownField != nil {
			onUnknownField(key)
		}
	}

	return record
}
