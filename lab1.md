### 实验准备

- 安装[go](https://go.dev/)环境

  `go version`(查看当前golang版本)

- 拉取实验代码

  `git clone git://g.csail.mit.edu/6.824-golabs-2022 6.824 `(代码在6.824这个目录下)

### 实验说明

这一节实验主要使用golang语言实现mapreduce论文的原型。在`src/main`目录下的`pg-*.txt`文件为输入，输出是对这些文件每个词出现次数的统计，也就是word count。实验开始前，老师已经提供了顺序计数代码的实现，为`src/main/mrsequential.go`和`src/mrapps/wc.go`。在测试的时候，首先会执行这两个文件：

```shell
# (在src/main/test-mr.sh中)
# generate the correct output
../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
```

生成正确的结果`mr-correct-wc.txt`。我们的任务是修改`src/mr`目录下的三个文件，然后测试脚本会调用我们编写的代码：

```shell
# (在src/main/test-mr.sh中)
echo '***' Starting wc test.

$TIMEOUT ../mrcoordinator ../pg*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
$TIMEOUT ../mrworker ../../mrapps/wc.so &
$TIMEOUT ../mrworker ../../mrapps/wc.so &
$TIMEOUT ../mrworker ../../mrapps/wc.so &
```

将我们编写代码的输出(`mr-out-*.txt`)与正确结果相比较(`mr-correct-wc.txt`)，以此判断代码正确性：

```shell
# (在src/main/test-mr.sh中)
# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi
```

综上所述，实验代码已经为我们提供了程序执行框架和测试脚本，我们只需要修改`src/mr`目录下的代码，然后执行测试脚本即可。那么，整个流程是怎样的呢？

程序入口为`src/main/mrcoordinator.go`，通过`mr.MakeCoordinator`初始化coordinator：

```go
import "6.824/mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
```

很容易看出，`src/main/mrcoordinator.go`为`src/mr/coordinator.go`的装饰器，即仅简单的封装调用，`src.main/mrworker`同理。执行`m.Done()`判断coordinator是否执行完毕。值得一提的是，若不做任何修改，执行测试脚本，则会一直卡住；若将`src/mr/coordinator.go`中的Done方法中的`ret:=flase`修改为`ret:=true`，则测试程序会立刻执行完毕，但结果无法通过。

### sequential代码解析

前面提到，`src/main/mrsequential.go`为顺序计数的实现，这里具体分析下是怎么实现的。

- 首先是map函数和reducer函数的加载(通过插件的方式)

  通过编译时添加`-buildmode=plugin`([什么是buildmode](https://chenjiehua.me/golang/golang-buildmode.html))，将代码编译为`.so`的形式，即动态链接库：

  ```shell
  # (在src/main/test-mr.sh中)
  (cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
  ```

  然后程序执行时，指定`.so`文件：

  ```shell
  # (在src/main/test-mr.sh中)
  ../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
  ```

  最后，代码实现中加载函数：

  ```golang
  mapf, reducef := loadPlugin(os.Args[1])
  ```

- 读取输入

  遍历每个文件，然后通过`mapf`将文件中的每个词按照`word, 1`的形式输出到`[]mr.KeyValue`中：

  ```golang
  intermediate := []mr.KeyValue{}
  	for _, filename := range os.Args[2:] {
  		file, err := os.Open(filename)
  		if err != nil {
  			log.Fatalf("cannot open %v", filename)
  		}
  		content, err := ioutil.ReadAll(file)
  		if err != nil {
  			log.Fatalf("cannot read %v", filename)
  		}
  		file.Close()
  		kva := mapf(filename, string(content))
  		intermediate = append(intermediate, kva...)
  	}
  ```

  对中间结果排序，将相同的word排序到一起：

  ```golang
  sort.Sort(ByKey(intermediate))
  ```

- 输出文件

  生成输出文件：

  ```golang
  oname := "mr-out-0"
  ofile, _ := os.Create(oname)
  ```

  找到每个word的所有`value`值，通过`recucef`统计每个词的次数，将结果输出：

  ```golang
  i := 0
  for i < len(intermediate) {
  	j := i + 1
  	for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
  		j++
  	}
  	values := []string{}
  	for k := i; k < j; k++ {
  		values = append(values, intermediate[k].Value)
  	}
  	output := reducef(intermediate[i].Key, values)
  
  	// this is the correct format for each line of Reduce output.
  	fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
  
  	i = j
  }
  ```

### rpc调用



### 测试一



