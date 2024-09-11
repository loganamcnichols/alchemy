from pandas.core.dtypes.dtypes import (
  register_extension_dtype,
  CategoricalDtype,
)
from pandas.core.arrays import Categorical
from pandas.core.ops.common import unpack_zerodim_and_defer
import numpy as np
from pandas.core import ops
from pandas.core.base import (
  ExtensionArray
)
from pandas.core.algorithms import (
  take_nd
)

from pandas.arrays import BooleanArray

from pandas.core.dtypes.common import (
  is_hashable,
  is_list_like,
  needs_i8_conversion,
)

from pandas import NA

from pandas._typing import type_t, Dtype

from pandas.core.dtypes.cast import coerce_indexer_dtype
import operator

def recode_for_categories(
    codes: np.ndarray, old_categories, new_categories, copy: bool = True
) -> np.ndarray:
    """
    Convert a set of codes for to a new set of categories

    Parameters
    ----------
    codes : np.ndarray
    old_categories, new_categories : Index
    copy: bool, default True
        Whether to copy if the codes are unchanged.

    Returns
    -------
    new_codes : np.ndarray[np.int64]

    Examples
    --------
    >>> old_cat = pd.Index(['b', 'a', 'c'])
    >>> new_cat = pd.Index(['a', 'b'])
    >>> codes = np.array([0, 1, 1, 2])
    >>> recode_for_categories(codes, old_cat, new_cat)
    array([ 1,  0,  0, -1], dtype=int8)
    """
    if len(old_categories) == 0:
        # All null anyway, so just retain the nulls
        if copy:
            return codes.copy()
        return codes
    elif new_categories.equals(old_categories):
        # Same categories, so no need to actually recode
        if copy:
            return codes.copy()
        return codes

    indexer = coerce_indexer_dtype(
        new_categories.get_indexer_for(old_categories), new_categories
    )
    new_codes = take_nd(indexer, codes, fill_value=-1)
    return new_codes


def _cat_compare_op(op):
    opname = f"__{op.__name__}__"
    fill_value = NA

    @unpack_zerodim_and_defer(opname)
    def func(self, other):
        hashable = is_hashable(other)
        if is_list_like(other) and len(other) != len(self) and not hashable:
            # in hashable case we may have a tuple that is itself a category
            raise ValueError("Lengths must match.")

        if not self.ordered:
            if opname in ["__lt__", "__gt__", "__le__", "__ge__"]:
                raise TypeError(
                    "Unordered Categoricals can only compare equality or not"
                )
        if isinstance(other, Categorical):
            # Two Categoricals can only be compared if the categories are
            # the same (maybe up to ordering, depending on ordered)

            msg = "Categoricals can only be compared if 'categories' are the same."
            if not self._categories_match_up_to_permutation(other):
                raise TypeError(msg)

            if not self.ordered and not self.categories.equals(other.categories):
                # both unordered and different order
                other_codes = recode_for_categories(
                    other.codes, other.categories, self.categories, copy=False
                )
            else:
                other_codes = other._codes
            mask = (self._codes == -1) | (other_codes == -1)
            return BooleanArray(op(self._codes, other_codes))

        if hashable:
            if other in self.categories:
                i = self._unbox_scalar(other)
                mask = self._codes == -1
                return BooleanArray(op(self._codes, i), mask)
            else:
                return ops.invalid_comparison(self, other, op)
        else:
            # allow categorical vs object dtype array comparisons for equality
            # these are only positional comparisons
            if opname not in ["__eq__", "__ne__"]:
                raise TypeError(
                    f"Cannot compare a Categorical for op {opname} with "
                    f"type {type(other)}.\nIf you want to compare values, "
                    "use 'np.asarray(cat) <op> other'."
                )

            if isinstance(other, ExtensionArray) and needs_i8_conversion(other.dtype):
                # We would return NotImplemented here, but that messes up
                #  ExtensionIndex's wrapped methods
                return op(other, self)
            return getattr(np.array(self), opname)(np.array(other))

    func.__name__ = opname

    return func

class NullableCategorical(Categorical):

    def __eq__(self, other):
        return _cat_compare_op(operator.eq)(self, other)
    
    def __ne__(self, other):
        return _cat_compare_op(operator.ne)(self, other)
    
    def __lt__(self, other):
        return _cat_compare_op(operator.lt)(self, other)
    
    def __gt__(self, other):
        return _cat_compare_op(operator.gt)(self, other)
    
    def __le__(self, other):
        return _cat_compare_op(operator.le)(self, other)
    
    def __ge__(self, other):
        return _cat_compare_op(operator.ge)(self, other)

    
@register_extension_dtype
class NullableCategory(CategoricalDtype):
    name = "nullable_category"

    @classmethod
    def construct_array_type(cls) -> type_t[NullableCategorical]:
        """
        Return the array type associated with this dtype.

        Returns
        -------
        type
        """

        return NullableCategorical