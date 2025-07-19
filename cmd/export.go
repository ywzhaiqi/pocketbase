package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/pocketbase/pocketbase/core"
	"github.com/spf13/cobra"
)

const (
	progressInterval = 2 * time.Second // 进度显示频率
	fileHeader       = "[\n"
	fileSeparator    = ",\n"
	fileFooter       = "\n]"
)

// NewExportCommand 创建导出命令
func NewExportCommand(app core.App) *cobra.Command {
	var pretty bool // 是否格式化 JSON 输出
	var batchSize int
	var outputFile string // 输出文件路径

	cmd := &cobra.Command{
		Use:   "export [集合名称]",
		Short: "导出指定集合的数据到JSON文件",
		Long:  `将指定集合的所有记录导出到JSON文件。支持大数据量分批处理。`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			collectionName := args[0]

			// 如果没有指定输出文件，使用默认名称
			if outputFile == "" {
				outputFile = fmt.Sprintf("%s_export.json", collectionName)
			}

			return exportData(app, collectionName, outputFile, pretty, batchSize)
		},
	}

	// 添加标志
	cmd.Flags().BoolVarP(&pretty, "pretty", "p", false, "是否格式化JSON输出")
	cmd.Flags().IntVarP(&batchSize, "batch-size", "b", 5000, "每批保存的记录数，默认5000")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "", "输出文件路径（默认为：集合名称_export.json）")

	return cmd
}

// exportData 处理数据导出的主流程
func exportData(app core.App, collectionName, outputFile string, pretty bool, batchSize int) error {
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
	if _, err := file.WriteString(fileHeader); err != nil {
		return fmt.Errorf("写入文件头部失败: %v", err)
	}

	// 初始化计数器和时间
	totalCount := 0
	startTime := time.Now()
	isFirstRecord := true

	// 分页查询参数
	page := 1
	perPage := batchSize
	hasMore := true

	// 用于安全退出进度显示 goroutine
	progressDone := make(chan struct{})
	progressTicker := time.NewTicker(progressInterval)
	defer progressTicker.Stop()

	// 启动进度显示协程
	go func() {
		for {
			select {
			case <-progressTicker.C:
				elapsed := time.Since(startTime)
				if totalCount > 0 {
					avgSpeed := float64(totalCount) / elapsed.Seconds()
					fmt.Printf("已处理: %d 条记录, 用时: %.1f秒, 平均: %.3f条/秒\n",
						totalCount, elapsed.Seconds(), avgSpeed)
				}
			case <-progressDone:
				return
			}
		}
	}()

	// 分批获取和处理记录
	for hasMore {
		records, err := app.FindRecordsByFilter(collection.Id, "", "", perPage, (page-1)*perPage)
		if err != nil {
			close(progressDone)
			return fmt.Errorf("获取记录失败: %v", err)
		}

		for _, record := range records {
			if err := writeRecordToFile(file, record, pretty, isFirstRecord); err != nil {
				close(progressDone)
				return err
			}
			isFirstRecord = false
			totalCount++
		}

		hasMore = len(records) == perPage
		page++
	}

	// 写入文件尾部
	if _, err := file.WriteString(fileFooter); err != nil {
		close(progressDone)
		return fmt.Errorf("写入文件尾部失败: %v", err)
	}

	// 停止进度显示
	close(progressDone)

	// 显示最终统计信息
	totalTime := time.Since(startTime)
	fmt.Printf("\n导出完成！\n")
	fmt.Printf("总记录数: %d\n", totalCount)
	fmt.Printf("总用时: %.1f秒\n", totalTime.Seconds())
	if totalCount > 0 {
		fmt.Printf("平均速度: %.3f条/秒\n", float64(totalCount)/totalTime.Seconds())
	}
	fmt.Printf("输出文件: %s\n", outputFile)

	return nil
}

// writeRecordToFile 将单条记录写入文件，处理分隔符和 JSON 编码
func writeRecordToFile(file *os.File, record any, pretty, isFirst bool) error {
	if !isFirst {
		if _, err := file.WriteString(fileSeparator); err != nil {
			return fmt.Errorf("写入分隔符失败: %v", err)
		}
	}
	var (
		jsonData []byte
		err      error
	)
	if pretty {
		jsonData, err = json.MarshalIndent(record, "  ", "  ")
	} else {
		jsonData, err = json.Marshal(record)
	}
	if err != nil {
		return fmt.Errorf("JSON编码失败: %v", err)
	}
	if _, err := file.Write(jsonData); err != nil {
		return fmt.Errorf("写入记录失败: %v", err)
	}
	return nil
}
