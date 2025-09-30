from .reduce import Reduce, UserPurchasesReducer, TpvReducer, QuantityReducer, ProfitReducer

class ReduceFactory:
    @staticmethod
    def create(reduce_type, mw_in, mw_out, output_exchange, output_rks, input_bindings):
        if reduce_type == "user_purchases":
            return UserPurchasesReducer(mw_in, mw_out, output_exchange, output_rks, input_bindings)
        elif reduce_type == "tpv":
            return TpvReducer(mw_in, mw_out, output_exchange, output_rks, input_bindings)
        elif reduce_type == "quantity":
            return QuantityReducer(mw_in, mw_out, output_exchange, output_rks, input_bindings)
        elif reduce_type == "profit":
            return ProfitReducer(mw_in, mw_out, output_exchange, output_rks, input_bindings)
        else:
            return Reduce(mw_in, mw_out, output_exchange, output_rks, input_bindings)
        
