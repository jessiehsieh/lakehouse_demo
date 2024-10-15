from unittest import TestCase

import pandas as pd
from pandas.testing import assert_frame_equal

from etl import compute_age_from_birthday
from etl import create_age_col_from_birthday, create_age_group_col_from_age, create_domain_col_from_email, flatten_df_column


class TestOffsetIdentification(TestCase):
    def test_compute_age_from_birthday(self):

        today_tuple = (2024,9,4)
        birthday_tuple = (1980,9,4)
        computed_age = compute_age_from_birthday(today_tuple, birthday_tuple)
        expected_age = 44

        self.assertEqual(computed_age, expected_age, 'Wrong age!')

        today_tuple = (2024,9,3)
        birthday_tuple = (1980,9,4)
        computed_age = compute_age_from_birthday(today_tuple, birthday_tuple)
        expected_age = 43

        self.assertEqual(computed_age, expected_age, 'Wrong age!')

        today_tuple = (2024,8,4)
        birthday_tuple = (1980,9,4)
        computed_age = compute_age_from_birthday(today_tuple, birthday_tuple)
        expected_age = 43

        self.assertEqual(computed_age, expected_age, 'Wrong age!')


    def test_create_age_col_from_birthday(self):
        
        input_df = pd.DataFrame({'birthday':['1980-09-03','1980-10-03','1980-09-04']})
        output_df = create_age_col_from_birthday(input_df, 
                                                 'birthday', 
                                                 'age', 
                                                 str_today='2024-09-03')
        expected_df = pd.DataFrame({'birthday':['1980-09-03','1980-10-03','1980-09-04'],
                                    'age':[44,43,43]})

        assert_frame_equal(output_df, expected_df)

    def test_create_age_group_col_from_age(self):
        
        input_df = pd.DataFrame({'age':[9, 18, 27, 36, 45, 54, 63, 72, 81, 90, 99, 108, 117]})
        output_df = create_age_group_col_from_age(input_df, 
                                                  'age', 
                                                  'age_group')
        expected_df = pd.DataFrame({'age':[9, 18, 27, 36, 45, 54, 63, 72, 81, 90, 99, 108, 117],
                                    'age_group': ['(0, 10]',
                                                  '(10, 20]',
                                                  '(20, 30]',
                                                  '(30, 40]',
                                                  '(40, 50]',
                                                  '(50, 60]',
                                                  '(60, 70]',
                                                  '(70, 80]',
                                                  '(80, 90]',
                                                  '(80, 90]',
                                                  '(90, 100]',
                                                  '(100, 110]',
                                                  '(110, 120]',
                                                  ]})

        assert_frame_equal(output_df, expected_df)


    def test_create_domain_col_from_email(self):
        
        input_df = pd.DataFrame({'email':['abc@yahoo.com','xyz@hotmail.com','ghi@aol.com']})
        output_df = create_domain_col_from_email(input_df, 'email', 'email_domain')
        expected_df = pd.DataFrame({'email':['abc@yahoo.com','xyz@hotmail.com','ghi@aol.com'],
                                    'email_domain':['yahoo.com','hotmail.com','aol.com']})

        assert_frame_equal(output_df, expected_df)

    def test_flatten_df_column(self):
        
        input_df = pd.DataFrame({'id':[1,2,3],
                                'nested':[{'id':1, 'inner':'value1'},
                                          {'id':1, 'inner':'value2'},
                                          {'id':1, 'inner':'value3'}]
                                })
        output_df = flatten_df_column(input_df, 'nested')
        expected_df = pd.DataFrame({'id':[1,2,3],
                                    'inner':['value1','value2','value3']
                                })

        assert_frame_equal(output_df, expected_df)