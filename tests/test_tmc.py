import unittest
import os
from tmc_summarizer.data_model import TMC_File

TESTDATA_FILENAME = os.path.join(os.path.dirname(
    __file__), '1_Cheltenham Ave _ Washington Ln.xls')


class test_TMC(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        """Sets up a TMC file once so the other tests can be run"""

        self.tmc = TMC_File('./1_Cheltenham Ave _ Washington Ln.xls')

    def test_tmc_name(self):
        self.assertTrue(self.tmc.location_name ==
                        '167385 - Cheltenham Ave & Washington Ln')

    def test_city(self):
        self.assertTrue(self.tmc.city_name == 'Cheltenham Twp')

    def test_legs(self):
        self.assertTrue(self.tmc.legs['NORTHBOUND STREET'] == 'Washington ln')
        self.assertTrue(self.tmc.legs['SOUTHBOUND STREET'] == 'Washington ln')
        self.assertTrue(
            self.tmc.legs['EASTBOUND STREET'] == 'PA 309 Cheltenham Ave')
        self.assertTrue(
            self.tmc.legs['WESTBOUND STREET'] == 'PA 309 Cheltenham Ave')

    def test_peak_hour(self):
        self.assertTrue(str(self.tmc.get_peak_hour(
            'am')[0] == '2023-05-24 07:15:00'))
        self.assertTrue(str(self.tmc.get_peak_hour(
            'am')[1] == '2023-05-24 08:15:00'))
        self.assertTrue(str(self.tmc.get_peak_hour(
            'pm')[0] == '2023-05-24 17:00:00'))
        self.assertTrue(str(self.tmc.get_peak_hour(
            'pm')[1] == '2023-05-24 18:00:00'))

# todo:
# -add another file to test network peak hour
# -test certain movements to make sure they're correctly pulling from network peak
# -test that dataframe comes in as expected
# -others?


if __name__ == '__main__':
    unittest.main()
