from tyc_aaoifi.run import run
import pandas as pd
pd.set_option('display.width', 5000)
pd.options.display.float_format = '{:,.2f}'.format
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

if __name__ == "__main__":
    run()