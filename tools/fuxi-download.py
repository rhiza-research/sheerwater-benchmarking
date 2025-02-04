from sheerwater_benchmarking.forecasts.fuxi import fuxi_raw
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-time", default="2016-01-01", type=str)
    parser.add_argument("--end-time", default="2022-12-31", type=str)
    args = parser.parse_args()

    ds = fuxi_raw(args.start_time, args.end_time, cache=False)
