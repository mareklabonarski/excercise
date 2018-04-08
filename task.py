import ast
import asyncio
import aiohttp
import re

import sys


async def get_file_content(url):
    """
    Gets content of a file at given url
    :param url: url of the file
    :return: file content
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            output = await response.text()
    print(output)
    return output


def parse_data(content):
    """
    creates a dict of data of following formats:
                    Team            P     W    L   D    F      A     Pts
            '   16. Bolton          38     9  13  16    44  -  62    40',
            '    3. Manchester_U    38    24   5   9    87  -  45    77',
              Dy MxT   MnT   AvT   HDDay  AvDP 1HrP TPcpn WxType PDir AvSp Dir MxS SkyC MxR MnR AvSLP
            '   9  86    32*   59       6  61.5       0.00         240  7.6 220  12  6.0  78 46 1018.6',
            '  12  88    73    81          68.7       0.00 RTH     250  8.1 270  21  7.9  94 51 1007.0',
            '  18  82    52    67          52.6       0.00         230  4.0 190  12  5.0  93 34 1021.3',
    :param content:
    :return: data dict where keys are column headers
    """
    patterns = [
        '^\s+(?P<Team>\d{1,2}\.\s+\w+)\s+(?P<P>\d{1,2})'
        '\s+(?P<W>\d{1,2})\s+(?P<L>\d{1,2})\s+(?P<D>\d{1,3})\s+(?P<F>\d{1,3})'
        '\s+-\s+(?P<A>\d{1,3})\s+(?P<Pts>.*)\s*',

        '^\s+(?P<Dy>\d{1,2})\s+(?P<MxT>\d{1,3})\*?\s+(?P<MnT>\d{1,3})\*?'
        '\s+(?P<AvT>\d{1,3})\s+(?P<HDDay>\d{1,3})?\s+(?P<AvDP>\d{1,3}\.\d)'
        '\s+(?P<HrP>\d{1,3})?\s+(?P<TPcpn>\d{1,3}\.\d{2})'
        '\s+(?P<WxType>[a-zA-Z]+)?\s+(?P<PDir>\d{1,3})\s+(?P<AvSp>\d{1,2}\.\d)'
        '\s+(?P<Dir>\d{1,3})\s+(?P<MxS>\d{1,3})\s+(?P<SkyC>\d{1,2}\.\d)'
        '\s+(?P<MnR>\d{1,3})\s+(?P<MxR>\d{1,3})\s+(?P<AvSLP>\d{1,4}\.\d)'
    ]
    datas = []
    for line in content.splitlines():
        for pattern in patterns:
            m = re.match(pattern, line)
            if m:
                data = m.groupdict()
                datas.append(data)
    return datas


def find_diff_extremum(datas, *keys, max_min=True):
    """
    Finds disctionary with extremum value of difference between values pointed by 2 keys
    :param datas: list of dictionaries
    :param keys: keys for calculating the difference
    :param max_min: whether max or min to be considered
    :return: dictionary with the extremum and value of the extremum
    """
    if not all([key in data] for key in keys for data in datas):
        raise Exception("Cannot find one of the keys on data provided")
    key = lambda data: (int(data[keys[0]]) - int(data[keys[1]]))
    datas = sorted(datas, key=key, reverse=bool(max_min))
    extremum_data = datas[0]
    try:
        return extremum_data, key(extremum_data)
    except IndexError:
        pass


async def print_extremum(url, diff_key1, diff_key2, report_key, max_min):
    """
    Prints extremum value of diff between 2 table columns under given url
    :param url: url of he table
    :param diff_key1: table column 1
    :param diff_key2: table column 2
    :param report_key: table column to be displayed for the row being extremum for col1 - col2
    :param max_min: calculate max or min (True or False)
    :return: nothing
    """
    content = await get_file_content(url)
    datas = parse_data(content)
    extremum_data, value = find_diff_extremum(datas, diff_key1, diff_key2, max_min=max_min)
    print(
        "Found %s of %s - %s = %s for record with %s = %s\n\n" % (
            max_min, diff_key1, diff_key2, value, report_key, extremum_data[report_key]
        )
    )


def main(*args):
    loop_args = [args[i:i + 5] for i in range(0, len(args), 5)]
    loop = asyncio.get_event_loop()
    coros = [asyncio.ensure_future(print_extremum(*args)) for args in loop_args]
    loop.run_until_complete(asyncio.wait(coros))
    loop.close()


if __name__ == '__main__':
    """
    Usage - pass subsets of arguments in order of:
    ulr column1 column2 column_for_report 1_or_0 
    """
    args = sys.argv[1:]
    args = [ast.literal_eval(arg) if not i % 5 else arg for i, arg in enumerate(args)]
    # if not given, use default
    args = args or [
        'http://codekata.com/data/04/weather.dat',
        'MxT', 'MnT', 'Dy', '0',
        'http://codekata.com/data/04/football.dat', 'F', 'A', 'Team', '0'
    ]
    main(*args)
