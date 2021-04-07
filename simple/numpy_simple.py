import numpy as np
# ndmin定义维度，ndim检查维护
# shape检查形状
# arr.copy()副本，arr.view()视图
# arr.reshape(1, 2, 3) 参数1定义数据维度，参数2定义每个维度的数组个数，参数3定义每个数据中的元素个数，可设为 -1 自动计算
# np.nditer(arr) 迭代(遍历)每个标量元素

arr = np.array([1, 2, 3, 4])

print(arr)
print(arr.ndim)
print('shape of array :', arr.shape)