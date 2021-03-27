# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

import ballista
from ballista import Field


class TestDataFrame(unittest.TestCase):

    def test_fields(self):
        fields = [
            Field("test_column_1", "int32", False),
            Field("test_column_2", "str", False),
        ]
        schema = ballista.Schema(*fields)
        print("HERE")


if __name__ == '__main__':
    unittest.main()