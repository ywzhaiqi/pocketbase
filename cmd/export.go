package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/pocketbase/pocketbase/core"
	"github.com/spf13/cobra"
)

// NewExportCommand 创建导出命令
func NewExportCommand(app core.App) *cobra.Command {
	var pretty bool // 是否格式化 JSON 输出

	cmd := &cobra.Command{
		Use:   "export [集合名称] [输出文件]",
		Short: "导出指定集合的数据到JSON文件",
		Long:  `将指定集合的所有记录导出到JSON文件。支持大数据量分批处理。`,
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			collectionName := args[0]
			outputFile := args[1]
			return exportData(app, collectionName, outputFile, pretty)
		},
	}

	// 添加格式化输出的标志
	cmd.Flags().BoolVarP(&pretty, "pretty", "p", false, "是否格式化JSON输出")

	return cmd
}

// exportData 处理数据导出的主流程
func exportData(app core.App, collectionName, outputFile string, pretty bool) error {
	// 获取目标集合
	collection, err := app.FindCollectionByNameOrId(collectionName)
	if err != nil {
		return fmt.Errorf("找不到集合 %s: %v", collectionName, err)
	}

	// 创建输出文件
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("创建输出文件失败: %v", err)
	}
	defer file.Close()

	// 写入文件头部
	file.WriteString("[\n")

	// 初始化计数器和时间
	totalCount := 0
	startTime := time.Now()
	isFirstRecord := true

	// 分页查询参数
	page := 1
	perPage := batchSize
	hasMore := true

	// 显示进度的计时器
	progressTicker := time.NewTicker(2 * time.Second)
	defer progressTicker.Stop()

	// 启动进度显示协程
	go func() {
		for range progressTicker.C {
			elapsed := time.Since(startTime)
			if totalCount > 0 {
				avgTime := elapsed.Seconds() / float64(totalCount)
				fmt.Printf("已处理: %d 条记录, 用时: %.1f秒, 平均: %.3f秒/记录\n",
					totalCount, elapsed.Seconds(), avgTime)
			}
		}
	}()

	// 分批获取和处理记录
	for hasMore {
		records, err := app.FindRecordsByFilter(collection.Id, "", "", perPage, (page-1)*perPage)
		if err != nil {
			return fmt.Errorf("获取记录失败: %v", err)
		}

		// 处理当前批次的记录
		for _, record := range records {
			// 添加记录分隔符
			if !isFirstRecord {
				file.WriteString(",\n")
			}
			isFirstRecord = false

			// 将记录转换为JSON并写入文件
			var jsonData []byte
			if pretty {
				jsonData, err = json.MarshalIndent(record, "  ", "  ")
			} else {
				jsonData, err = json.Marshal(record)
			}
			if err != nil {
				return fmt.Errorf("JSON编码失败: %v", err)
			}
			file.Write(jsonData)

			totalCount++
		}

		// 检查是否还有更多记录
		hasMore = len(records) == perPage
		page++
	}

	// 写入文件尾部
	file.WriteString("\n]")

	// 停止进度显示
	progressTicker.Stop()

	// 显示最终统计信息
	totalTime := time.Since(startTime)
	fmt.Printf("\n导出完成！\n")
	fmt.Printf("总记录数: %d\n", totalCount)
	fmt.Printf("总用时: %.1f秒\n", totalTime.Seconds())
	if totalCount > 0 {
		fmt.Printf("平均速度: %.3f秒/记录\n", totalTime.Seconds()/float64(totalCount))
	}
	fmt.Printf("输出文件: %s\n", outputFile)

	return nil
}
