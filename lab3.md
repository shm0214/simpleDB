<center><h3>Lab3 B+ Tree Index</h3></center>
<div align='right'>学号： 1911463 姓名: 时浩铭 </div>

- 代码仓库：https://github.com/shm0214/simpleDB
- 提交记录：

[![gN8LK1.png](https://z3.ax1x.com/2021/05/10/gN8LK1.png)](https://imgtu.com/i/gN8LK1)

- 对接口没有改动

-----

`lab3`中，主要完善了大部分代码已经写好了的`BTree`，需要我们完成`findLeafPage`、`splitLeafPage`、`splitInternalPage`以及删除的一些函数。

B+树的具体实现较为复杂，其`Page`分为`BTreeHeaderPage`、`BTreeInternalPage`、`BTreeLeafPage`与`BTreeRootPtrPage`四种。B+树中每一个节点实质上就是一个`Page`。他们都继承自抽象类`BTreePage`，可以通过通过`BTreePageId.pgcateg`得到具体的`Page`类型。关于这四个`Page`的具体实现也不用过分深究，如果只是完成实验的话，大致了解相关函数用法即可。

下面具体看练习代码。

-----

### Search

主要完成`findLeafPage`函数。这个函数会根据传入的字段，根据内部节点的索引值递归调用，直到找到叶节点为止。

函数接受开始递归查找的`PageId`，首先要判断这个`Page`是否是`InternalPage`，如果是的话通过`BTreeFile.getPage`来得到这个`InternalPage`。接着要看要查找的`Field`是否为空，如果为空则根据文档，递归调用其第一个子节点即可。然后便需要遍历这个`InternalPage`中存储的`BTreeEntry`来确定下次遍历的`Page`。具体的方法即为利用`iterator`来找到第一个比`Field`大的`Entry`即可，找到后通过`entry.getLeftChild`即可得到其左孩子（即下次遍历的`Page`）的`PageId`，递归调用即可。如果遍历完也没有找到比`Field`大的`Entry`，则递归调用最后一个子节点即可。

-----

### Insert

主要完成`splitLeafPage`与`splitInternalPage`即可。

这两个函数都只完成了向右分裂一个`Page`的功能，并且关于父子关系的维护已经有其他函数写好，只需要调用即可，因此难度不大。

先看`splitLeafPage`。

首先需要得到一个新的`Page`用于分裂，方法是`getEmptyPage`。之后再取被分裂`Page`的`iterator`，用于遍历`Tuple`，从而把后半部分的`Tuple`移到新的`Page`中，具体方法只需要`deleteTuple`再`insertTuple`即可。这样就完成了第一步，还需要将这两个被修改过的`Page`加入`dirtyPages`中。

之后维护一下叶子节点间的指针，只需要分别设置一下左右节点的`PageId`即可。

最后再维护与父节点的联系。首先要再父节点中插入一个新的`Entry`，其值是新加入的`Page`的第一个`Tuple`的`key`。查找父节点时，只需要调用`getParentWithEmptySlots`即可得到父节点，然后再调用`insertEntry`即可完成插入。而关于父节点的分裂等则不需要在此考虑。其次再设置一下新的`Page`的`ParentId`即可。

最后由于我们返回的是含有`field`的`Page`，因此比较一下看`field`是在原`Page`还是在新的`Page`中，并返回正确的`Page`即可。

再看`splitInternalPage`。

大体上与叶子节点的分裂类似，区别在于从原`Page`删除的是`Entry`，并且删除时要使用`deleteKeyAndRightChild`同时删除与子节点的联系，同时在新的`Page`增加`Entry`时通过`updateParentPointer`来维护与子节点的联系。而且`middleKey`不需要加入到新`Page`中，需要单独保存`middleEntry`用于向父节点中增加`Entry`。

之后需要设置`middleKey`的左右孩子，并将其插入到父节点即可。

也需要将这三个`Page`加入`dirtyPages`。

最后同样需要判断`field`在哪个`Page`中用于返回。

------

### Delete

需要完成三个`steal`函数和两个`merge`函数。

`steal`函数就是从兄弟节点中借`Tuple`/`Entry`过来同时需要维护父子节点。`merge`函数就是合并两个节点得到一个新的节点，同时也需要维护父子节点。

与Insert的函数类似，也是只需要调用写好的其他函数即可。

先看`stealFromLeafPage`。

这个函数中，通过一个布尔值`isRightSibling`来确定从左兄弟还是右兄弟借用`Tuple`。借`Tuple`的具体实现也比较简单，只需要一个循环，条件是这个`Page`的`Tuple`数目小于兄弟节点的`Tuple`数目，循环内只需要从兄弟节点删除一个再添加到这个节点即可。注意需要根据`isRightSibling`来得到正确方向的迭代器。

循环结束后，需要修改父节点中的`Entry`的值，其值应该为右侧节点的第一个`Tuple`的`key`值，修改完后调用`updateEntry`即可。

再看`stealFromLeftInternalPage`。

与叶子节点类似，区别在于需要每次更新父节点中的`Entry`，需要把多的节点的`Entry`替换父节点的`Entry`，并把父节点的`Entry`加入到少的节点之中。同时也需要更新父子节点之间的指针。

`stealFromRightInternalPage`无太大区别，不再赘述。

再看`mergeLeafPage`。

合并是把右侧节点合并到左侧节点之中，具体操作也很简单，删除右侧节点中`Tuple`并加入左侧即可。之后需要维护叶子节点之间的联系。并且右侧的`Page`需要用`setEmpthPage`标记为空。并更新父节点中的`Entry`。最后把修改过的三个`Page`加入`dirtyPages`即可。

最后看`mergeInternalPage`。

与叶子节点的合并也类似，区别在于孩子节点指针的移动和父节点中`Entry`的下拉，即需要`deleteParentEntry`。具体不再赘述。

----

### `BTreeReverseScan`

实现一个逆向的扫描。可以参照`BTreeScan`的实现。

具体只需要实现几个`Reverse`的`iterator`和函数即可。

具体包括`reversedFindLeafPage`、`indexReverseIterator`、`reverseIterator`、`BTreeFileReverseIterator`和`BTreeReversedSearchIterator`。

这些具体实现也很简单，只需要互换原来代码中的`left`/`right`和`reverseXXX`/`XXX`即可。`BTreeReverseScan`也是如此。