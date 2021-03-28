<center><h3>Lab1 SimpleDB</h3></center>
<div align='right'>学号： 1911463 姓名: 时浩铭 </div>

- 代码仓库：https://github.com/shm0214/simpleDB
- 提交记录：[![cp9hX4.png](https://z3.ax1x.com/2021/03/28/cp9hX4.png)](https://imgtu.com/i/cp9hX4)
- 对接口没有改动。

-----

`Lab1`主要实现数据库的底层存储结构，主要有`Tuple`、`TupleDesc`、`Catalog`、`HeapPage`、`HeapFile`等。首先简单分析下主要的存储层次。

- `Tuple`和`TupleDesc`是一张数据表中的基本元素，前者表示除表头外的每一行数据，是若干个属性的元祖。后者则是一张表的`metadata`，包括每一列元素的名称和类型
- `Catalog`则是一个目录，存储着数据库中所有的表和他们的`schema`
- `BufferPool`是一个缓存池，类似于`cache`，用来做`Page`的读取的加速。读取`Page`时首先会访问`BufferPool`，不存在则会访问`File`，并将得到的`Page`存入`BufferPool`
- `HeapPage`就是`Page`的一个简单实现，每个`Page`中存储着一些具有相同`TupleDesc`的`Tuple`。`HeapFile`是`File`的一个简单实现，每个`File`中存储着许多`HeapPage`

简单的说就是，`File`中储存`Page`，`Page`中存储许多模式相同的`Tuple`，模式叫做`TupleDesc`。访问`Tuple`优先会从缓存`BufferPool`中获取，不存在则再从`File`中获取。我们常说的数据库中的`Table`在`SimpleDB`中就是一个`File`。

下面具体分析。

------

### Fields and Tuples 

主要实现`TupleDesc.java`和`Tuple.java`两个类。

前者存储数据表的`metadata`，包括类型和名称。其中定义了`TDItem`类，表示一个列，包括类型和名称。因此我们只需要创建和维护一个`TDItem`的列表即可。

```java
private List<TDItem> TDList;
```

之后再根据所给的提示，完成迭代器的实现。

其中构造器包含许多不同参数的构造器，依次按照所给参数完成。这里为了方便后面第六个练习的实现，多写一个拷贝构造函数，注意`List`的深拷贝。

剩余一些获取`FieldName`和`FieldType`的函数就按照要求完成，注意检验参数范围，不合适则抛出异常即可。

注意在`fieldNameToIndex`函数中，对`List`遍历要用`String`的`equals`方法，因此要对参数和对应`fieldName`做是否为空的检查，否则无法调用`equals`方法。

`TupleDesc`的`equals`方法按照所给注释，既要检验个数，又要检验对应的`TDItem`是否相等。还要注意传入参数是`Object`类型，要先转换成`TupleDesc`。

其余方法按照注释即可实现。

后者是存储表中数据项的元祖。

构造器传入`TupleDesc`，因此要有对应字段接受。同时创建储存数据的`Field`数组，在构造器中开辟相应空间，具体大小由`TupleDesc.numFileds()`方法传入。

`RecordId`记录着特定表的特定页的特定数据项。由于存在`getter`和`setter`，因此也设置字段。

`setField`和`getField`只需要注意下标的边界检查。

`toString`的实现按照注释要求，为方便可利用一个`StringBuilder`或`StringBuffer`对象。

迭代器的实现也容易，简单的对`Field`数组的迭代器。

`resetTupleDesc`函数只需要new一个新的`TupleDesc`即可。

----

### Catalog 

主要实现`Catalog.java`类。`Catalog`中存储着所有的`Table`，文档中告诉我们访问的方法`Database.getCatalog()`。

为了方便后面操作，我们定义一个内部类`Table`。

```java
private class Table {
    public DbFile file;
    public String name;
    public String pkeyField;
    public int id;
}
```

成员变量来自后面`addTable`的函数签名。这里为了方便后面获取`Table`的`id`，多增加一个字段，构造器中初始化为`file.getId()`。

`Catalog`同样用`List`来存储`Table`，同时由于后面有`tableIdIterator()`，同时维护一个`idList`。

构造器中初始化两个`List`即可。

`addTable`方法中，需要根据注释处理几种情况，`name`为空和`name`冲突以及`id`冲突的情况。冲突遍历检查，若存在则用后面的替代即可。`name`为空，则与在`exercise`中一致，先判断是否为空再调用`equals`方法。

`getTableId`只需要遍历列表即可，同样注意由于以`name`查询，需要判空。其余`getter`均遍历即可。

`tableIdIterator`暂时不懂是做什么的，但我们有`idList`，返回其迭代器即可。

`clear`方法中清空两`list`即可。

----

### BufferPool

主要实现`BufferPool.java`类。`BufferPool`是`Page`的缓存。查看下面方法可知，查询`Page`是以`PageId`来查询，因此我们存储`Page`的数据结构显然就是`HashMap<PageId, Page>`。

构造器传入`BufferPool`能存储的最大`Page`数，因此创建一字段接受。构造器中初始化`HashMap`。

`getPage`方法传入三个参数，但只用到了`PageId`，剩下的可能是后面要修改。实现也很简单，首先在`HashMap`中检索，不存在则到文件中检索，并存储在`BufferPool`中。根据文档，目前若`BufferPool`已满不需替换，只需抛出`DbException`即可。到文件中检索，首先需要找到文件，`DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());`，在利用`file.readPage`方法即可得到`Page`。

-----

### HeapFile access method 

主要实现`HeapPageId.java`、`RecordId.java`和`HeapPage.java`类。前两个都是简单的`Id`，前者是`Tuple`的，后者是`Page`的。`HeapPage`则是`Page`的简单实现，在其中需要考虑`Page`的存储细节。

两个`Id`实现都很简单，根据构造器传入参数设立字段，然后实现`hashCode`和`equals`方法即可。

这里先分析下`HeapFile`的存储结构，每个`HeadFile`中存着一组`HeadPage`。在`HeadFile`中每个`Page`是一个`slot`。每个`HapPage`前还有一个`header`，是一个`bitmap`，用于指示每个`Tuple`是否有效。

由于`header`的存在，因此每个`Tuple`占据空间为`tupleSize * 8 + 1`。`header`的大小可以通过`Tuple`的数目计算。

关于`bitmap`的存储，是从低位先开始存储，并且注意`Java`虚拟机的大端字节序。

`getNumTuples`和`getHeaderSize`按照所给公式实现即可。注意`Math.ceil()`返回`double`型。

`isSlotUsed`就是通过查看`header`的`bitmap`去看这个`slot`是否已经被占用。首先计算对应`bit`是在哪个`byte`里，再计算再这个`byte`中哪一位。由于从低位存储，因此所在`byte`左移`7 - position`位，并与`0x80`做与操作。如果得到字节只有首位为1，则说明已经被占用。

`getNumEmptySlots`就遍历`header`的`bitmap`，利用`isSlotUsed`来判断有多少为空，计数即可。

最后再实现下`Tuple`的迭代器。返回的是所有有效的`Tuple`，因此在`next`中需要利用`isSlotUsed`找到下一个有效的`Tuple`。`hasNext`中利用所有`Tuple`的数量和有效的`Tuple`数量判断。

-----

#### HeapFile

主要实现`HeapFile.java`类。`HeapFile`中`HeapPage`的存储是乱序的。

根据构造器，设置字段`File`和`TupleDesc`。

`getId`中根据所给注释，`Id`的设置采用`file.getAbsoluteFile().hashCode()`。

`readPage`需要我们去文件中读取数据，然后返回一个`Page`。首先确定`Page`在文件中的偏移，然后利用`RandomAccessFile`读取。偏移量的计算首先要知道`PageSize`，还需要知道`tableId`，这个定义在`PageId`之中。计算好偏移量后就可以移动文件指针，然后读取数据到一个`byte[]`中，再传入`HeadPage`的构造器即可得到`HeapPage`对象。

`numPages`类似于上一节中也是简单计算即可，用文件的大小除以每一个`Page`的大小即可得到数目。

这个实验中最复杂的在于`DbFileIterator`的实现。这个迭代器返回的是每一个`Tuple`，但`Tuple`是存储在`Page`中。因此首先要遍历`Page`，再遍历`Tuple`。

由于较为复杂且需要传参，不采用匿名内部类实现。

设置内部类`HeapFileIterator`实现`DbFileIterator`借口。字段有 `Iterator<Tuple>`，`TransactionId`和一个`pagePosition`用于表示遍历到的`Page`数目。

首先实现一个`getTupleIterator()`。文档中要求我们通过`BufferPool`来访问`Page`，因此需要用到`Database.getBufferPool().getPage()`。其参数需要一个`HeapPageId`，首先创建一个`HeapPageId`对象。再根据`pagePosition`找到对应的`HeapPage`，再返回之前实现的`iterator`即可。

`open`函数首先初始化`pagePosition`，再调用`getTupleIterator()`初始化`iterator`即可。

`hasNext`中需要首先检查当前`Page`中有没有下一个`Tuple`，再检查当前`File`中有没有下一个`Page`。前者检查只需要看`iteator.hasNext`，后者比较`pagePosition`和`numPages()`。如果当前`Page`读取完毕，则改变`pagePosition`后重新`getIterator()`即可。

`next`先检查`hasNext`，然后只需调用`iterator.next()`就行了。

`rewind`就重新`open`一下就行。

`close`就把`iterator = null`即可。

最后再说明一下`BufferPool`和`HeapFile`。两个类是相互调用的关系，应该是为了一定先从`BufferPool`中读取`Page`，所以才要求`File`中都要先调`BufferPool`，不存在则再从`BufferPool`中调`File`。

-----

#### Operators 

只要实现`SeqScan.java`类。就实现简单的对`Tuple`的线性遍历，主要就封装一下上一节实现的`iterator`。

根据构造器设置字段，完成初始化。同时为了方便遍历，设置一个`DbFileIterator`，并在构造器中完成赋值。

一些`getter`就按要求返回即可。

这里`getTupleDesc`比较复杂，因为`name`要加上`alias.`。利用新写的拷贝构造函数再遍历`tdItem`逐一修改即可。这里注意字符串判空，若为空显示一个`"null"`。

余下函数简单实现即可。

-----

通过系统测试，完成`Lab1`。

[![cpCqVs.png](https://z3.ax1x.com/2021/03/28/cpCqVs.png)](https://imgtu.com/i/cpCqVs)

