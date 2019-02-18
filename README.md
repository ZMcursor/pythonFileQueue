# pythonFileQueue

这是一个基于文件系统的队列，提供和python内置的Queue相同的API，用来解决队列过长从而占用大量内存的问题。

这个问题出现在我的头条自媒体号爬虫程序中，初期我是直接使用python内置的Queue，但元素的产生速度远大于消耗速度，在爬取几天后队列长度就超过1000万，直到达到近5000万，此时队列占用内存已经超过1G。于是想到先将队列的一部分缓存到硬盘，从而减轻内存占用，因此编写了这个工具类，使用这个工具类替代内置的Queue后，队列内存占用几乎可以不计。
### FileQueue的原理很简单，但也实用：
- 维护两个队列（使用collections.deque）queue_in,queue_out
- queue_in用来添加元素，queue_out用来取出元素
- 当queue_in长度等于给定的buffer size时，将queue_in序列化（使用pickle模块）保存到文件，并将queue_in清空
- 当queue_out为空时，从文件中反序列化获得一个新queue
- 类维护了元素的顺序，保证了FIFO
