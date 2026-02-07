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

// NewImportCommand 创建导入命令
func NewImportCommand(app core.App) *cobra.Command {
	var batchSize int
	cmd := &cobra.Command{
		Use:   "import [json文件路径] [集合名称]",
		Short: "导入JSON数据到指定集合",
		Long: `从JSON文件导入数据到指定的集合中。支持以下格式：
1. 标准JSON数组格式
2. 格式化的JSON（支持多行）
3. 每行一个JSON对象

如果未指定集合名称，将从JSON文件名中自动提取集合名称（支持以下格式）：
- xxx_export_2024-01-01.json -> xxx
- xxx.json -> xxx`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("缺少JSON文件路径参数")
			}
			if len(args) > 2 {
				return fmt.Errorf("参数过多，最多接受2个参数：JSON文件路径和可选的集合名称")
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
			return importData(app, jsonFile, collectionName, batchSize)
		},
	}
	cmd.Flags().IntVarP(&batchSize, "batch-size", "b", 5000, "每批保存的记录数，默认5000")
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
func importData(app core.App, jsonFile, collectionName string, batchSize int) error {
	// 获取目标集合
	collection, err := app.FindCollectionByNameOrId(collectionName)
	if err != nil {
		return fmt.Errorf("找不到集合 %s: %v", collectionName, err)
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
			return importJSONArray(app, reader, collection, batchSize)
		} else {
			return importJSONLines(app, reader, collection, batchSize)
		}
	}
}

// importJSONArray 流式导入标准JSON数组
func importJSONArray(app core.App, reader *bufio.Reader, collection *core.Collection, batchSize int) error {
	dec := json.NewDecoder(reader)
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
		record := mapToRecord(item, collection)
		return record, false, nil
	}
	return processBatchInsert(app, batchSize, recordGenerator)
}

// importJSONLines 流式导入每行一个JSON对象
func importJSONLines(app core.App, reader *bufio.Reader, collection *core.Collection, batchSize int) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, maxLineSize), maxLineSize)
	lineNum := 0
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
			record := mapToRecord(item, collection)
			return record, false, nil
		}
		if err := scanner.Err(); err != nil {
			return nil, true, fmt.Errorf("文件读取错误: %v", err)
		}
		return nil, true, nil
	}
	return processBatchInsert(app, batchSize, recordGenerator)
}

// processBatchInsert 通用批量插入逻辑，减少重复
// recordGenerator: 每次调用生成一个 *core.Record 和 bool（是否结束）
// totalHint: 预估总数（如无法预估可传0）
func processBatchInsert(app core.App, batchSize int, recordGenerator func() (*core.Record, bool, error)) error {
	records := make([]*core.Record, 0, batchSize)
	totalCount := 0
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
		records = append(records, record)
		totalCount++
		if len(records) >= batchSize {
			batch++
			if err := saveRecordsBatch(app, records, batch, totalCount); err != nil {
				return err
			}
			records = make([]*core.Record, 0, batchSize)
		}
	}
	if len(records) > 0 {
		batch++
		if err := saveRecordsBatch(app, records, batch, totalCount); err != nil {
			return err
		}
	}
	totalTime := time.Since(startTime)
	if totalCount > 0 && totalTime.Seconds() > 0 {
		avgSpeed := float64(totalCount) / totalTime.Seconds()
		fmt.Printf("\n导入完成！总记录数: %d, 总用时: %.3f秒, 平均: %.3f条/秒\n", totalCount, totalTime.Seconds(), avgSpeed)
	} else {
		fmt.Printf("\n导入完成！总记录数: %d, 总用时: %.3f秒, 平均: -\n", totalCount, totalTime.Seconds())
	}
	return nil
}

// saveRecordsBatch 统一批量保存逻辑，增强日志和进度
func saveRecordsBatch(app core.App, records []*core.Record, batchNum, totalCount int) error {
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
		return fmt.Errorf("批量保存失败: %v", err)
	}

	fmt.Printf("成功导入第%d批数据，共%d条记录，累计导入%d条\n", batchNum, len(records), totalCount)
	return nil
}

// mapToRecord 辅助函数：map转Record，处理created/updated
// item: 原始数据map
// collection: 目标集合
// 返回: *core.Record
func mapToRecord(item map[string]any, collection *core.Collection) *core.Record {
	record := core.NewRecord(collection)
	for key, value := range item {
		if key == "created" || key == "updated" {
			record.SetRaw(key, value)
		} else {
			record.Set(key, value)
		}
	}
	return record
}
