from .join import Join, MenuJoin, StoreJoin, UserJoin


class JoinFactory:
    @staticmethod
    def create(join_type, mw_in, mw_out, output_exchange, output_rks, input_bindings, storage):
        if join_type == "MenuJoin":
            return MenuJoin(mw_in, mw_out, output_exchange, output_rks, input_bindings, storage)
        elif join_type == "Store":
            return StoreJoin(mw_in, mw_out, output_exchange, output_rks, input_bindings, storage)
        elif join_type == "User":
            return UserJoin(mw_in, mw_out, output_exchange, output_rks, input_bindings, storage)
        else:
            return Join(mw_in, mw_out, output_exchange, output_rks, input_bindings, storage)