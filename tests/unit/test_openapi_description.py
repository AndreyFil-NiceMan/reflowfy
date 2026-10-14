from fastapi import FastAPI

from reflowfy.api.routes import create_pipeline_routes
from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.core.runtime_params import RuntimeParams


class _Described(AbstractPipeline[RuntimeParams]):
    name = "described_openapi"
    description = "Moves widgets from A to B."

    def define_source(self, runtime_params):  # type: ignore[no-untyped-def]
        raise NotImplementedError

    def define_transformations(self, records, runtime_params):  # type: ignore[no-untyped-def]
        return []

    def define_destination(self, runtime_params):  # type: ignore[no-untyped-def]
        raise NotImplementedError


def test_description_in_swagger():
    app = FastAPI()
    create_pipeline_routes(app, _Described(), None, None)
    paths = app.openapi()["paths"]
    run = paths["/pipelines/described_openapi/run"]["post"]
    assert run["description"].startswith("Moves widgets from A to B.")
    assert paths["/pipelines/described_openapi/status"]["get"]["description"] == (
        "Moves widgets from A to B."
    )


def test_no_description_is_fine():
    class _Plain(_Described):
        name = "plain_openapi"
        description = ""

    app = FastAPI()
    create_pipeline_routes(app, _Plain(), None, None)
    run = app.openapi()["paths"]["/pipelines/plain_openapi/run"]["post"]
    assert run["description"].startswith("Execute plain_openapi")
    assert AbstractPipeline.description == ""


def test_rate_limit_default_in_swagger():
    class _Limited(_Described):
        name = "limited_openapi"
        rate_limit = 3000

    app = FastAPI()
    create_pipeline_routes(app, _Limited(), None, None)
    params = app.openapi()["paths"]["/pipelines/limited_openapi/run"]["post"]["parameters"]
    rl = next(p for p in params if p["name"] == "rate_limit")
    assert "Pipeline default: 3000" in rl["description"]
