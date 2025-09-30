from .map import Map, MapYearMonth, MapYearHalf


class MapFactory:
    @staticmethod
    def create(map_type, mw_in, mw_out, output_exchange, output_rks, input_bindings):
        if map_type == "YearMonth":
            return MapYearMonth(mw_in, mw_out, output_exchange, output_rks, input_bindings)
        if map_type == "YearHalf":
            return MapYearHalf(mw_in, mw_out, output_exchange, output_rks, input_bindings)
        else:
            raise ValueError(f"Invalid map type: {map_type}")