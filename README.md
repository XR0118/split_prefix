# split_prefix

## 使用方式

Usage of ./tire:

-cl int (各个子树的前缀数量限制，越小子树前缀分得越细，建议是一个比较小的值)

count limit when get result of small part, should be small enough for result accuracy (default 1000)

-fp string (是用工具拉取 bucket 保存结果的路径, 需要以 / 结尾)

directory for files need to split prefix

-l int (统计结果的前缀数量限制, 想获取 bucket 下前缀文件数 2000 以内的前缀, 该值即为 2000)

desired limit for file with the same prefix (default 10000000)

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
在文件读取完毕后，根据 countLimit 来筛选前缀。

## radix-counter

常用的 radix 树，每个节点都有自己的叶子节点。
叶子节点记录当前前缀的数量，用于最后的统计。
统计时根据 limit 进行筛选。
