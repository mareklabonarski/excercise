import unittest
import asyncio

from task import parse_data, get_file_content, main, find_diff_extremum, print_extremum


def run_async(coro):
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(coro(*args, **kwargs))

    return wrapper


class TestFileDownload(unittest.TestCase):
    @run_async
    async def test_get_file_content(self):
        url = 'http://codekata.com/data/04/weather.dat'
        downloaded_content = await get_file_content(url)

        with open('weather.dat', 'r') as f:
            reference_content = f.read()

        self.assertEqual(
            downloaded_content, reference_content,
            'Downloaded file content is not equal to reference'
        )


class TestTask(unittest.TestCase):
    def test_find_diff_extremum(self):
        input_datas = [
            [[[{'A': 2, 'B': 2}, {'A':4, 'B': 1}], 'A', 'B', True], [{'A':4, 'B': 1}, 3]],
            [[[{'C': 2, 'D': 2}, {'C': 4, 'D': 1}], 'C', 'D', False], [{'C':2, 'D': 2}, 0]]
        ]
        for (datas, *keys, max_min), expected in input_datas:
            calculated = find_diff_extremum(datas, *keys, max_min=max_min)
            self.assertEqual(tuple(expected), calculated)

    @run_async
    async def test_main(self):
        args = [
            ['http://codekata.com/data/04/weather.dat',
            'MxT', 'MnT', 'Dy', 0],
            ['http://codekata.com/data/04/football.dat', 'F', 'A', 'Team', 0]
        ]
        for _args in args:
            await print_extremum(*_args)


class TestParser(unittest.TestCase):

    def test_parsing_weather(self):
        lines = [
            '   16. Bolton          38     9  13  16    44  -  62    40',
            '    3. Manchester_U    38    24   5   9    87  -  45    77',

            '   9  86    32*   59       6  61.5       0.00         240  7.6 220  12  6.0  78 46 1018.6',
            '  12  88    73    81          68.7       0.00 RTH     250  8.1 270  21  7.9  94 51 1007.0',
            '  18  82    52    67          52.6       0.00         230  4.0 190  12  5.0  93 34 1021.3',
            '  26  97*   64    81          70.4       0.00 H       050  5.1 200  12  4.0 107 45 1014.9'
        ]

        datas = parse_data('\n'.join(lines))
        print(datas)
        self.assertEqual(
            datas, [
                {'Team': '16. Bolton', 'P': '38', 'W': '9', 'L': '13', 'D': '16', 'F': '44', 'A': '62', 'Pts': '40'},
                {'Team': '3. Manchester_U', 'P': '38', 'W': '24', 'L': '5', 'D': '9', 'F': '87', 'A': '45',
                 'Pts': '77'},
                {'Dy': '9', 'MxT': '86', 'MnT': '32', 'AvT': '59', 'HDDay': '6', 'AvDP': '61.5', 'HrP': None,
                 'TPcpn': '0.00', 'WxType': None, 'PDir': '240', 'AvSp': '7.6', 'Dir': '220', 'MxS': '12',
                 'SkyC': '6.0',
                 'MnR': '78', 'MxR': '46', 'AvSLP': '1018.6'},
                {'Dy': '12', 'MxT': '88', 'MnT': '73', 'AvT': '81', 'HDDay': None, 'AvDP': '68.7', 'HrP': None,
                 'TPcpn': '0.00', 'WxType': 'RTH', 'PDir': '250', 'AvSp': '8.1', 'Dir': '270', 'MxS': '21',
                 'SkyC': '7.9',
                 'MnR': '94', 'MxR': '51', 'AvSLP': '1007.0'},
                {'Dy': '18', 'MxT': '82', 'MnT': '52', 'AvT': '67', 'HDDay': None, 'AvDP': '52.6', 'HrP': None,
                 'TPcpn': '0.00', 'WxType': None, 'PDir': '230', 'AvSp': '4.0', 'Dir': '190', 'MxS': '12',
                 'SkyC': '5.0',
                 'MnR': '93', 'MxR': '34', 'AvSLP': '1021.3'},
                {'Dy': '26', 'MxT': '97', 'MnT': '64', 'AvT': '81', 'HDDay': None, 'AvDP': '70.4', 'HrP': None,
                 'TPcpn': '0.00', 'WxType': 'H', 'PDir': '050', 'AvSp': '5.1', 'Dir': '200', 'MxS': '12', 'SkyC': '4.0',
                 'MnR': '107', 'MxR': '45', 'AvSLP': '1014.9'}
            ]
        )
