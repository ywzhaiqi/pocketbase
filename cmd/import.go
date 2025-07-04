package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pocketbase/pocketbase/core"
	"github.com/spf13/cobra"
)

const (
	batchSize   = 5000             // 每批处理的记录数
	maxLineSize = 10 * 1024 * 1024 // 10MB，单行最大大小
)

// NewImportCommand 创建导入命令
func NewImportCommand(app core.App) *cobra.Command {
	return &cobra.Command{
		Use:   "import [json文件路径] [集合名称]",
		Short: "导入JSON数据到指定集合",
		Long: `从JSON文件导入数据到指定的集合中。支持以下格式：
1. 标准JSON数组格式
2. 格式化的JSON（支持多行）
3. 每行一个JSON对象`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return importData(app, args[0], args[1])
		},
	}
}

// importData 处理数据导入的主流程
func importData(app core.App, jsonFile, collectionName string) error {
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

	// 自动识别格式
	reader := bufio.NewReader(file)
	for {
		b, err := reader.Peek(1)
		if err != nil {
			return fmt.Errorf("读取文件失败: %v", err)
		}
		if b[0] == ' ' || b[0] == '\n' || b[0] == '\r' || b[0] == '\t' {
			_, _ = reader.ReadByte() // 跳过空白
			continue
		}
		if b[0] == '[' {
			// JSON数组格式
			return importJSONArray(app, reader, collection)
		} else {
			// 每行一个JSON对象格式
			return importJSONLines(app, reader, collection)
		}
	}
}

// importJSONArray 流式导入标准JSON数组
func importJSONArray(app core.App, reader *bufio.Reader, collection *core.Collection) error {
	dec := json.NewDecoder(reader)
	// 跳过数组开始
	t, err := dec.Token()
	if err != nil {
		return fmt.Errorf("读取JSON文件头失败: %v", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("JSON文件不是以数组开头: %v", t)
	}

	var (
		records    = make([]*core.Record, 0, batchSize)
		totalCount = 0
		batch      = 0
	)

	for dec.More() {
		var item map[string]any
		if err := dec.Decode(&item); err != nil {
			return fmt.Errorf("解析JSON对象失败: %v", err)
		}
		record := mapToRecord(item, collection)
		records = append(records, record)
		totalCount++
		if len(records) >= batchSize {
			batch++
			if err := saveBatch(app, records, batch); err != nil {
				return err
			}
			records = make([]*core.Record, 0, batchSize)
		}
	}
	if len(records) > 0 {
		batch++
		if err := saveBatch(app, records, batch); err != nil {
			return err
		}
	}
	fmt.Printf("成功导入 %d 条记录\n", totalCount)
	return nil
}

// importJSONLines 流式导入每行一个JSON对象
func importJSONLines(app core.App, reader *bufio.Reader, collection *core.Collection) error {
	scanner := bufio.NewScanner(reader)
	var (
		records    = make([]*core.Record, 0, batchSize)
		totalCount = 0
		batch      = 0
		lineNum    = 0
	)
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var item map[string]any
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			return fmt.Errorf("第%d行解析失败: %v", lineNum, err)
		}
		record := mapToRecord(item, collection)
		records = append(records, record)
		totalCount++
		if len(records) >= batchSize {
			batch++
			if err := saveBatch(app, records, batch); err != nil {
				return err
			}
			records = make([]*core.Record, 0, batchSize)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("文件读取错误: %v", err)
	}
	if len(records) > 0 {
		batch++
		if err := saveBatch(app, records, batch); err != nil {
			return err
		}
	}
	fmt.Printf("成功导入 %d 条记录\n", totalCount)
	return nil
}

// mapToRecord 辅助函数：map转Record，处理created/updated
func mapToRecord(item map[string]any, collection *core.Collection) *core.Record {
	record := core.NewRecord(collection)
	for key, value := range item {
		record.Set(key, value)
	}
	if created, ok := item["created"].(string); ok {
		if t, err := time.Parse(time.RFC3339, created); err == nil {
			record.SetRaw("created", t)
		}
	}
	if updated, ok := item["updated"].(string); ok {
		if t, err := time.Parse(time.RFC3339, updated); err == nil {
			record.SetRaw("updated", t)
		}
	}
	return record
}

// saveBatch 批量保存记录
func saveBatch(app core.App, records []*core.Record, batchNum int) error {
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

	fmt.Printf("成功导入第%d批数据，共%d条记录\n", batchNum, len(records))
	return nil
}
