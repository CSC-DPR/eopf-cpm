from typing import Any, Iterable

import numpy as np
from numpy.typing import DTypeLike

from eopf.computing.abstract import EOBlockProcessingStep
from eopf.product import EOVariable


class FlagEvaluationStep(EOBlockProcessingStep):
    """
    Flag variable interpretation step example implementation
    to demonstrate the chaining of processing steps.
    """

    SYNTAX_TOKEN = ["AND", "OR", "NOT"]

    def apply(  # type: ignore[override]
        self, variable: EOVariable, dtype: DTypeLike = float, **kwargs: Any
    ) -> EOVariable:
        flag_masks = variable.attrs.get("flag_masks")
        flag_meanings = variable.attrs.get("flag_meanings")

        if flag_masks is None or flag_meanings is None:
            raise ValueError(f"Not a flag variable, required attributes are missing. ({flag_masks=}, {flag_meanings=})")

        flag_meanings_list = flag_meanings.split(" ")
        if (len_meanings := len(flag_meanings_list)) != (len_masks := len(flag_masks)):
            raise ValueError(f"Flags masks and meanings must have the same length: {len_meanings} != {len_masks}.")

        flags = dict(zip(flag_meanings_list, flag_masks))
        return super().apply(variable, dtype=dtype, flags=flags, **kwargs)

    def func(  # type: ignore[override]
        self,
        data: np.ndarray[Any, np.dtype[Any]],
        flags: dict[str, Any] = {},
        flag_expression: str = "",
    ) -> np.ndarray[Any, np.dtype[Any]]:
        """
        Block-wise interpretation of a flag expression .
        :param inputs: flag band data as the first and only member
        :param kwargs: flag_expression: logical expression made up of flag_meanings and logical operators
        :return: numpy array with block of boolean values
        """

        # @todo add support for braces () tb 2022-03-17
        tokens = flag_expression.split(" ")
        for token in tokens:
            if not self._is_valid_token(token, extra_token=flags.keys()):
                raise ValueError(f"invalid token detected in expression: {token}")

        # TODO remove after types are preserved by reader
        if data.dtype == np.float64:
            data = np.array(data, dtype=np.uint32)

        # This code should probably be extended to build up a complete parse-tree.
        # The general logic here is the any combination of flags with AND or AND NOT
        # can be handled by
        #     mask_array = flag_array & select == reduce
        # where "select" is the OR combination of all flags in the expression and
        # "reduce" is the OR combination of all non-negated flags
        # For OR combinations we need to build a separate layer of masks that is pushed on the
        # stack. The final boolean mask is the created by ORing all layers in the stack.
        select = 0
        reduce = 0
        not_set = False
        stack = []
        for operator in tokens:
            if operator == "NOT":
                not_set = True
                continue

            if operator == "AND":
                continue

            if operator == "OR":
                layer_mask = data & select == reduce
                stack.append(layer_mask)
                select = 0
                reduce = 0
                continue

            select = select | flags[operator]
            if not not_set:
                reduce = reduce | flags[operator]
            not_set = False

        mask = data & select == reduce
        for layer_mask in stack:
            mask = mask | layer_mask
        return mask

    def _is_valid_token(self, token: str, extra_token: Iterable[str] = []) -> bool:
        return token in [*extra_token, *self.SYNTAX_TOKEN]
