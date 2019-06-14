# split_prefix

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
