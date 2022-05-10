from typing import Any, Sequence

import numpy as np

from eopf.computing.abstract import BlockProcessingStep


class InterpolateTpStep(BlockProcessingStep):
    """
    Interpolation of tie-point grid to image grid.
    Implementation borrowed from xcube resampling. TODO replace by import
    """

    def func(  # type: ignore[override]
        self,
        tp_data: np.ndarray[Any, np.dtype[Any]],
        tp_step: Sequence[int] = tuple(),
        block_id: Sequence[int] = tuple(),
        shape: Sequence[int] = tuple(),
        chunksize: Sequence[int] = tuple(),
    ) -> np.ndarray[Any, np.dtype[Any]]:
        """
        Block-wise interpolation of tie-point data to the image grid
        using linear interpolation based on pixel coordinates (not geo-coordinates).
        :param inputs: ignored, a dummy 1*1 array is sufficient
        :param block_id: tuple of (block_row, block_col)
        :param kwargs: tp_data: numpy array, complete tp grid extent
                       tp_step: tuple of tie point step in y and x
                       shape: tuple of image shape
                       chunksize: tuple of image chunksize
        :return: numpy array of interpolated tp_data with the extent of the image block,
                 usually image_chunksize except for the right and lower border.
        """
        # extend tp grid by one column and one row for interpolation
        tp_height, tp_width = tp_data.shape
        tp_y_step, tp_x_step = tp_step
        tp_dummy_column = np.zeros((tp_height, 1))
        tp_dummy_row = np.zeros(tp_width + 1)

        tp_data = np.vstack([np.hstack([tp_data, tp_dummy_column]), tp_dummy_row])
        # corner pixel coordinates of the block
        y = self.process(block_id[0], chunksize[0], shape[0]).T
        x = self.process(block_id[1], chunksize[1], shape[1])

        y_tp = y // tp_y_step
        x_tp = x // tp_x_step
        wy = (y - y_tp * tp_y_step) / tp_y_step
        wx = (x - x_tp * tp_x_step) / tp_x_step

        # 2-D interpolation using numpy broadcasting
        return (
            (1 - wy) * (1 - wx) * tp_data[y_tp, x_tp]
            + (1 - wy) * wx * tp_data[y_tp, x_tp + 1]
            + wy * (1 - wx) * tp_data[y_tp + 1, x_tp]
            + wy * wx * tp_data[y_tp + 1, x_tp + 1]
        )

    def process(self, block_id: int, chunksize: int, shape: int) -> np.ndarray[Any, np.dtype[Any]]:
        item = block_id * chunksize
        item_bis = min(item + chunksize, shape)
        return np.arange(item, item_bis).reshape(1, (item_bis - item))
