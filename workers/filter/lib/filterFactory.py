from .filter import Filter, AmountFilter, YearFilter, HourFilter


class FilterFactory:
    @staticmethod
    def create(filter_type, mw, result_mw, input_rk, output_rk):
        if filter_type == "Amount":
            return AmountFilter(mw, result_mw, input_rk, output_rk)
        elif filter_type == "Year":
            return YearFilter(mw, result_mw, input_rk, output_rk)
        elif filter_type == "Hour":
            # tiene que recibir mas parametros porque tiene dos input exchange
            return HourFilter(mw, result_mw, input_rk, output_rk)
        else:
            return Filter(mw, result_mw, input_rk, output_rk)
