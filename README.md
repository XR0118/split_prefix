# split_prefix

## 使用方式

Usage of ./tire:

-fp string (是用工具拉取 bucket 保存结果的路径, 需要以 / 结尾)

directory for files need to split prefix

-l int (统计结果的前缀数量限制, 想获取 bucket 下前缀文件数 2000 以内的前缀, 该值即为 2000)

desired limit for file with the same prefix (default 20000000)

-sl int (对拉的大文件进行分割的文件行数)

split large file to the specified number (default 1000000)

-sp string (保存结果的文件, 文件可以创建, 但是路径必须要存在)

result save path

-upl int (前缀限制的上限，默认 40000000, 可以根据需要修改)

up-limit for file with the same prefix (default 40000000)

## file

counter.go: 对给定的文件夹下的文件进行前缀统计

file.go: 主要进行文件操作：分割、读、创建、移动

## radix

统计各个文件中不同前缀数量使用的 radix 树，去掉了叶子节点，增加了每个节点的计数。
在文件读取完毕后，根据前缀数量来筛选前缀。

## 性能

一次读入内存的文件名数量最多控制在 200W，根据 split limit 进行 pool 大小的计算

|    文件数   | 时间 | 内存 |
| ---------- | --- | ---- |
|     1亿    |  215s |  1G |
