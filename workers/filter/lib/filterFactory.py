from .filter import Filter, AmountFilter, YearFilter, HourFilter


class FilterFactory:
    @staticmethod
    def create(filter_type, mw_in, mw_out, output_exchange, output_rks, input_bindings):
        if filter_type == "Year":
            return YearFilter(mw_in, mw_out, output_exchange, output_rks, input_bindings)
        elif filter_type == "Hour":
            return HourFilter(mw_in, mw_out, output_exchange, output_rks, input_bindings)
        elif filter_type == "Amount":
            return AmountFilter(mw_in, mw_out, output_exchange, output_rks, input_bindings)
        else:
            return Filter(mw_in, mw_out, output_exchange, output_rks, input_bindings)