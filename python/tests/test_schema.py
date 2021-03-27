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
import json

class TestSchema(unittest.TestCase):
    
    def test_fields(self):
        fields = [
            Field("test_column_0", "str", False),
            Field("test_column_1", "i16", False),
            Field("test_column_2", "i32", False),
            Field("test_column_3", "f32", False),
            Field("test_column_4", "f64", False),
            Field("test_column_5", "bool", False),
            Field("test_column_6", "date32", False),
            Field("test_column_7", "date64", False)
        ]
        schema = ballista.Schema(*fields)
        schema_json = json.loads(schema.to_json()).get("fields")
        types = list(map(lambda f: f.get("data_type"), schema_json))
        expect = ["Utf8", "Int16", "Int32", "Float32", "Float64", "Boolean", "Date32", "Date64"]
        self.assertEqual(types, expect)

if __name__ == '__main__':
    unittest.main()