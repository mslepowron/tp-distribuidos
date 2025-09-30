from .top import Top,TopSellingItems, TopRevenueGeneratinItems


class TopFactory:
    @staticmethod
    def create(top_type, mw_in, mw_out, output_exchange, output_rks, input_bindings):
        if top_type == "TopSellingItems":
            return TopSellingItems(mw_in, mw_out, output_exchange, output_rks, input_bindings)
        elif top_type == "TopRevenueGeneratinItems":
            return TopRevenueGeneratinItems(mw_in, mw_out, output_exchange, output_rks, input_bindings)
        else:
            raise ValueError(f"Invalid top type: {top_type}")