<center><h3>Lab1 SimpleDB</h3></center>
<div align='right'>学号： 1911463 姓名: 时浩铭 </div>

- 代码仓库：https://github.com/shm0214/simpleDB
- 提交记录：[![czuY0H.png](https://z3.ax1x.com/2021/04/25/czuY0H.png)](https://imgtu.com/i/czuY0H)](https://imgtu.com/i/cW5yLT)
- 对接口没有改动。

-----

`Lab2`中，主要实现了几个操作符例如`SELECT`、`JOIN`等。此外还完善了`Lab1`中`Bufferpool`中未实现的替换算法。

下面具体说明

----

### `Join&Filter`

实现两个操作符`Filter`和`Join`。前者遍历数据表返回满足条件的元祖，后者返回连接得到的数组。

需要实现四个类`Predicate`、`JoinPredicate`、`Filter`和`Join`。

-----

`Predicate`是一个辅助类，定义了一种比较的方式，可以传入`tuple`来返回是否满足条件，便于后面`Filter`的实现。

里面首先定义了比较所需的具体操作符类，如`=`、`>`等。

构造器接受三个参数，参与比较的字段编号、操作符和比较的字段，因此也创建变量接受。

一些`getter`就不再赘述。

`filter`判断是否满足比较条件，并返回`boolean`，调用`Field`的`compare`方法即可。

`toString`按照注释返回即可。

----

`Filter`类实现`Operator`接口，实质上是在`Filter`中调用`Predicate`去比较，并返回满足条件的`Tuple`。

构造器接受两个参数，谓词和一个遍历被筛选的元祖的迭代器，创建变量接受。

`open`中要先`child.open`再`super.open`。同样关闭时要先`super.close`再`child.close`。`rewind`时就只需要`child.rewind`即可。

`fetchNext`就是实现筛选的关键函数，在这之中逐一遍历`child.next()`，直到找到一个满足条件的`tuple`返回。由于每次调用只返回一个，因此同时`child`的位置不应变化，从而方便下次遍历。

`getter`和`setter`不再赘述。

-----

`JoinPredicate`与`Predicate`类似。

构造器接受两个字段和操作符，创建变量接受。

`filter`也是调用`compare`来进行比较。

----

`Join`类也实现`operator`接口，原理与`Filter`类似，但比`Filter`更复杂一些。

构造器接受`JoinPredicate`和两个`child`，创建变量接受。

`getTupleDesc`返回的也是连接后的`tupleDesc`。连接使用静态方法`TupleDesc.merge`即可。由于下面`fetchNext`时需要频繁使用，因此创建一个变量保存。

`Join`比`Filter`复杂主要是因为需要同时遍历两个数据表。 遍历时先保持其中一个不变，遍历另一个。等另一个遍历完后，再移动第一个。因此需要设置一个`flag`来判断何时去移动第一个，还需要暂时存储一下第一个的`Tuple`。

`open`、`close`以及`rewind`中除调用父类和`child`外，还需要设置一下`flag`和暂存的`tuple`。

为了更好的实现`fetchNext`，先实现一个函数`mergeTuple`，接受参数是被连接的两个`tuple`。只需要先根据`TupleDesc`创建一个`Tuple`，再分别遍历两个`Tuple`把字段存进去即可。

`fetchNext`中先根据`flag`判断是否需要切换第一个`tuple`。之后再遍历第二个`tuple`，通过`joinPredicate.filter`来判断是否满足，满足则调用`mergeTuple`返回。如果第二个遍历完了也没有找到的话，就切换第一个并调用`child2.rewind`即可。

-----

### `Aggregates`

`Aggregate`是实现聚合操作，如`group by`中的`avg`、`max`等操作。只需要实现`IntergerAggregator`和`StringAggregator`两种类型的操作。

-----

`StringAggregator`比较简单，只支持`COUNT`，先实现这个。

由于需要分组，我们用`HashMap`来保存每一组的结果。如果有分组依据，则用`Field`做`key`，否则以`null`做`key`。

构造器中先判断`op`是不是`COUNT`，之后接受参数并保存并初始化`HashMap`。接受参数有分组依据的字段编号，分组依据的字段类型，计数的字段编号和操作符。

`MergeTupleIntoGroup`接受一个新的`tuple`，然后将其对应的结果更新在`HashMap`中。

`iterator()`返回一个遍历得到的聚合操作的结果的迭代器。由于比较复杂，单独实现一个`StringAggregateIterator`类。保存一个`HashMap`的迭代器和一个`TupleDesc`。

构造器中需要根据有没有分组依据来设置`tupleDesc`，有的话返回的`tuple`有两列，否则有一列，并设置对应的列名。`next`中就从`HashMap`中取一个，根据是否有`key`来设置`tuple`的一个/两个字段返回即可。其余方法不再赘述。

-----

`IntegerAggregator`相比于`StringAggregator`较为复杂，主要是需要实现的操作较多。处理方法就是多弄几个`HashMap`，然后根据操作数类型来初始化对应的`HashMap`。操作数是`AVG`时，需要同时维护`sumMap`和`countMap`，以便于得到平均数。

在`IntegerAggregateIterator`的`open`中，也根据不同的操作数来设置迭代器是哪一个`HashMap`的迭代器。

其余只需要类比`StringAggregator`即可。

-----

`Aggregate`实现了`Operator`接口，返回的是执行`group by`得到的表。

构造器接受需要遍历的表的迭代器，聚合依据的字段编号和被操作的字段编号以及操作数，创建变量保存即可。

还需要保存一个遍历结果的迭代器。

`open`函数比较复杂，主要是根据是`Integer`还是`String`有所区分，而且还对是否有分组依据有所区分。这不同的四类情况下对应初始化`Aggregator`即可。之后再遍历需要分组的表，一个一个调用`aggregator.mergeTupleIntoGroup`即可。最后初始化迭代器为`aggregator.iterator`即可。

`getTupleDesc`返回的也是结果的`TupleDesc`，也需要分类讨论。

其余函数不再赘述。

-----

### `HeapFile Mutability`

下面要实现对于表的修改，包括删除和添加。删除时根据`RecordId`来找到储存在哪个`Page`上，再删除并清空`header`。添加时需要找到一个有空的`slot`的`Page`再添加，如果没有则需要新建一个`HeapPage`，并把它保存在`HeapFile`里。

-----

先实现`HeapPage.java`。

删除操作需要先根据`RecordId`找到存储在`Page`中的编号。然后需要判断一些条件，比如`pageId`是否与这个`Page`一样，比如这个`slot`是否被占用，比如这个`tuple`是否已经被删除，比如存储的是否是这个`tuple`。判断最后一个时需要注意`tuple`没有提供`equals`方法，需要自己重写。判断无误后只需要把对应的`slot`标记为0即可。

`markSlotUsed`与之前检索`slot`的过程类似，但包括了置位和取消两个操作。可以先设置一个`mask`只有`byte`的对应位置1，然后如果置位只需要或操作，如果取消，则去`~mark`与操作即可。

插入时，要先查看是否还有空的`slot`，然后还要查看`tupleDesc`是否匹配。最后再遍历`tuple`数组找到第一个空的`tuple`，在设置一个新的`recordId`，保存`pageId`和`tupleNumber`，最后再标记`slot`即可。

除此之外，根据文档，还需要实现`markDirty`和`IsDirty`。因此需要两个变量，一个保存是否`Dirty`，另一个保存上一次被修改时的`TransactionId`。

-----

在实现`HeapFile.java`。

删除时，需要先根据`tuple`获取`RecordId`。然后再找到对应的`HeapPage`，这里需要从`BufferPool`里来取，调用`getPage`，传入`TransactionId`和`PageId`即可得到，权限设置为读写。然后调用`page.deleteTuple`即可。由于返回的是链表，因此初始化一个链表并存入被修改的`Page`返回。

增加时，需要依次遍历所有的`page`判断是否有空的`slot`，有的话只需要调用`page.insertTuple`并添加到链表返回即可。如果所有的`Page`都没有，则需要新创建一个`Page`，`HeapPage`提供了静态方法，提供了一个空的`Page`的数据，直接写入对应的`file`即可。之后再调用新的`Page`的`insertTuple`方法即可。

-----

最后再实现`BufferPool`中的添加与删除。

删除时，通过`Page`返回的链表依次标记`BufferPool`中的`Page`为`Dirty`，再将其保存在`pageHashMap`中。

添加时，与删除同理。

-----

下面理一下这三个文件的逻辑，调用关系

```
BufferPool ==> HeapFile ==> HeapPage
```

删除时只调用`BufferPool`，然后他会调用`HeapFile`，并根据返回的链表来设置脏位，并重新存入`HashMap`。`HeapFile`则会找到对应的`HeapPage`来完成相关操作。

-----

### `Insertion and deletion`

实现增加和删除两个操作符，主要是调用上面实现的类。

---

`Insert.java`中构造器接受`transactionId`、被插入的元祖的迭代器和`tableId`。根据注释需要先检查`TupleDesc`是否匹配。

因为插入后要返回一下插入了多少个元祖，因此也要设立一个`TupleDesc`来保存这个格式，一个`count`变量来计数，并在构造器中初始化。

`fetchNext`就执行了插入操作，并返回数量。注释说只有第一次执行有效，因此还要有一个`flag`来记录是否是第一次。然后就是遍历被插入的元祖，调用`BufferPool.insertTuple`就行了。最后返回一个记录数量的表。

其余方法不再赘述。

----

`Delete.java`与插入基本类似。值得注意的是这里的`fetchNext`并没有要求只有第一次有效，但是实际上需要的。

----

### `Page eviction`

设计`BufferPool`中的替换算法。

-----

首先需要先补充一下之前`HeapFile`中的`writePage`，因此被替换的页需要把文件中的同步更新。首先检查`pageId`与`File`中的`Page`总数。再用`RandomAccessFile`移动到相应位置，把传入的`Page`的数据写入相应位置即可。

-----

替换算法的实现采用随机替换法，每次从`HashMap`中随机选取一个替换出去。需要先把`key`转成数组，再利用`random.randInt`获得一个下标，从而获得被删除的`key`，然后调用`flushPage`把对应内容写回到文件，只有调用`discard`删除。

`flushPage`就是把传入的`Page`的内容写回到文件的对应位置，根据传入的`pageId`找到对应的文件调用其`writePage`即可。最后把`dirty`标记取消。

`discardPage`从`BufferPool`中直接删除而不写回到文件，因此只需要删除`HashMap`中的键值对即可。

`flushAllPages`会找到所有的`dirty`的`Page`并写回到文件，遍历所有`Page`调用`flushPage`即可。

最后再把报缺页异常的地方用`evictPage`处理即可。