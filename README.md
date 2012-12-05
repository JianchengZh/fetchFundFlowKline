fetchFundFlowKline
==================

抓取资金流向的历史k线
Usage: index [options] 

  Options:

    -h, --help              output usage information
    -V, --version           output the version number
    -k, --key <n>           要删除的key
    -t, --type <n>          操作类型是删除(del)还是加载历史数据(load)
    -r --redis <n>          redis地址
    -l --thread length <n>  启用的线程数默认是8个
    -p --data path <n>      资金流向历史数据的备份文件
    -c --trim count <n>     从尾开始按需截取list类型数据

  Examples:

    1:删除数据(支持模糊删除)
      $ node index.js -r 172.16.33.203:6390 -k MLINE.SH600000 -l 10 -t del 

    2:加载资金流向K线历史数据
      $ node index.js -r 172.16.33.203:6390 -p /home/cool/data -l 10 -t load 

    3:从尾开始按需截取list类型数据(支持模糊截取)
      $ node index.js -r 172.16.33.203:6390 -k KEMCF.MAIN.SH* -c 120 

    [注意] 操作类型-t一定要放在最后

