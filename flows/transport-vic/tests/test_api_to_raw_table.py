# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Validation tests for the API to Raw Table flow.

Structural tests validate the flow definition JSON without a running instance.
"""

import json
import os

import pytest


FLOW_PATH = os.path.join(os.path.dirname(__file__), "..", "api-to-raw-table.json")
PARAMETER_CONTEXT = "REST API Source"


@pytest.fixture
def flow_definition():
    with open(FLOW_PATH, "r") as f:
        return json.load(f)


@pytest.fixture
def parameters(flow_definition):
    context = flow_definition["parameterContexts"][PARAMETER_CONTEXT]
    return {p["name"]: p for p in context["parameters"]}


class TestFlowStructure:
    def test_flow_is_valid_json(self, flow_definition):
        assert flow_definition is not None

    def test_has_snapshot_metadata(self, flow_definition):
        assert "snapshotMetadata" in flow_definition
        metadata = flow_definition["snapshotMetadata"]
        assert metadata["bucketIdentifier"] == "transport-vic"
        assert metadata["flowIdentifier"] == "api-to-raw-table"
        assert "version" in metadata

    def test_has_processors(self, flow_definition):
        contents = flow_definition["flowContents"]
        assert len(contents["processors"]) == 3

    def test_has_connections(self, flow_definition):
        contents = flow_definition["flowContents"]
        assert len(contents["connections"]) >= 1

    def test_processors_have_required_fields(self, flow_definition):
        contents = flow_definition["flowContents"]
        for processor in contents["processors"]:
            assert "identifier" in processor
            assert "name" in processor
            assert "type" in processor
            assert "bundle" in processor

    def test_expected_processor_types_present(self, flow_definition):
        contents = flow_definition["flowContents"]
        processor_types = {p["type"] for p in contents["processors"]}
        assert "org.apache.nifi.processors.standard.InvokeHTTP" in processor_types
        assert "org.apache.nifi.processors.jolt.JoltTransformJSON" in processor_types
        assert "com.snowflake.openflow.runtime.processors.snowpipe.PutSnowpipeStreaming" in processor_types

    def test_api_response_routed_to_transform(self, flow_definition):
        contents = flow_definition["flowContents"]
        routes = [
            c for c in contents["connections"]
            if c["source"]["name"] == "Get API data" and c["destination"]["name"] == "Specify JSON structure"
        ]
        assert len(routes) == 1
        assert "Response" in routes[0]["selectedRelationships"]

    def test_transform_routed_to_snowflake(self, flow_definition):
        contents = flow_definition["flowContents"]
        routes = [
            c for c in contents["connections"]
            if c["source"]["name"] == "Specify JSON structure" and c["destination"]["name"] == "Insert data into Snowflake"
        ]
        assert len(routes) == 1
        assert "success" in routes[0]["selectedRelationships"]


class TestParameters:
    def test_parameter_context_present(self, flow_definition):
        assert PARAMETER_CONTEXT in flow_definition["parameterContexts"]
        assert flow_definition["flowContents"]["parameterContextName"] == PARAMETER_CONTEXT

    def test_expected_parameters_present(self, parameters):
        expected = {"api_url", "KeyId", "snowflake_database", "snowflake_schema", "snowflake_table", "snowflake_role"}
        assert expected.issubset(parameters.keys())

    def test_api_key_is_sensitive(self, parameters):
        assert parameters["KeyId"]["sensitive"] is True

    def test_no_sensitive_values_committed(self, parameters):
        for name, param in parameters.items():
            if param["sensitive"]:
                assert not param.get("value"), "Sensitive parameter '%s' must not have a value in the flow JSON" % name
