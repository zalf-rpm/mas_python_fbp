"""create_monica_env, converted from Runnable to Process style (plan LP3)."""

from __future__ import annotations

import json

import pytest
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.geo import geo_capnp
from mas.schema.grid import grid_capnp
from mas.schema.model.monica import monica_management_capnp as mgmt_capnp
from mas.schema.model.monica import sim_setup_capnp

from tests.component_harness import (
    PortMessage,
    PortValue,
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.models.monica.create_monica_env import METADATA, CreateMonicaEnv

SIM = {
    "include-file-base-path": "/tmp",
    "debug?": False,
    "climate.csv": "",
    "climate.csv-options": {"no-of-climate-file-header-lines": 1, "csv-separator": ","},
    "output": {"events": [], "obj-outputs?": True},
}
SITE = {
    "SiteParameters": {"Latitude": 1.0, "heightNN": 0.0, "slope": 0.0},
    "EnvironmentParameters": {"AtmosphericCO2": 380.0},
    "SoilMoistureParameters": {},
    "SoilTemperatureParameters": {},
    "SoilTransportParameters": {},
    "SoilOrganicParameters": {},
}
CROP = {
    "CropParameters": {},
    "cropRotation": [
        {
            "worksteps": [
                {
                    "type": "Sowing",
                    "date": "2020-03-01",
                    "crop": {
                        "cropParams": {
                            "species": {},
                            "cultivar": {"StageTemperatureSum": [[100, 200, 300]]},
                        }
                    },
                },
                {"type": "Harvest", "date": "2020-09-01"},
            ]
        },
        "unused",
        "crop-id-slot",
    ],
}


@pytest.fixture
def templates(tmp_path):
    """The sim/crop/site trio a Setup points at, written out as real files."""

    paths = {}
    for name, content in (("sim", SIM), ("crop", CROP), ("site", SITE)):
        path = tmp_path / f"{name}.json"
        path.write_text(json.dumps(content))
        paths[name] = str(path)
    return paths


def setup_message(templates, **overrides):
    fields = {
        "runId": 1,
        "cropId": "WW",
        "simJson": templates["sim"],
        "cropJson": templates["crop"],
        "siteJson": templates["site"],
    }
    fields.update(overrides)
    return sim_setup_capnp.Setup.new_message(**fields)


def env_ip(templates, *, lat=52.0, lon=13.0, setup=None, **attrs):
    """An IP carrying the run's values in its attributes, as the component expects."""

    named: dict = {
        "latlon": geo_capnp.LatLonCoord.new_message(lat=lat, lon=lon),
        "setup": setup if setup is not None else setup_message(templates),
    }
    named.update(attrs)
    named = {k: v for k, v in named.items() if v is not None}
    ip = fbp_capnp.IP.new_message()
    entries = ip.init("attributes", len(named))
    for i, (key, value) in enumerate(named.items()):
        entries[i].key = key
        entries[i].value = value
    return PortMessage(PortValue(ip))


def run(messages, **settings):
    inputs: dict = {"in": [*messages, done_message()]}
    if settings:
        inputs["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    component = CreateMonicaEnv(METADATA)
    result = run_process_component(component, inputs=inputs, outputs=("out",))
    return result, component


def standard(writer):
    return [v for v in writer.values if str(v.type) == "standard"]


def env_json(result, index=0) -> dict:
    """The MONICA JSON out of the Env. `rest` is an AnyPointer, so it needs an explicit cast."""

    from mas.schema.model import model_capnp

    env = standard(result.output("out"))[index].content.as_struct(model_capnp.Env)
    return json.loads(env.rest.as_struct(common_capnp.StructuredText).value)


def test_it_is_a_process_component_now() -> None:
    assert METADATA.type == "process"


def test_it_emits_an_env_at_all(templates) -> None:
    """The Runnable version emitted nothing for two independent reasons: it built the output with
    `common_capnp.IP`, which does not exist, and its config keys were declared as 'dgm_attr',
    'setup_attr' and so on while the code looked up 'dgm', 'setup', 'coord' - so the very first
    `if "coord" in config` was always false and every IP hit `continue`."""

    result, _ = run([env_ip(templates)])
    assert len(standard(result.output("out"))) == 1


def test_the_env_is_tagged_and_carries_monica_json(templates) -> None:
    result, _ = run([env_ip(templates)])
    out = standard(result.output("out"))[0]
    assert out.sysAttributes.contentType == "model.capnp:Env"
    assert env_json(result)["type"] == "Env"


def test_the_custom_id_records_the_run(templates) -> None:
    result, _ = run([env_ip(templates, lat=51.0, lon=12.0)])
    custom = env_json(result)["customId"]
    assert custom["setup_id"] == 1
    assert custom["crop_id"] == "WW"
    assert (custom["lat"], custom["lon"]) == (51.0, 12.0)


def test_an_id_attribute_is_used_when_present(templates) -> None:
    result, _ = run([env_ip(templates, id="run-7")])
    assert env_json(result)["customId"]["id"] == "run-7"


def test_a_uuid_is_generated_when_no_id_attribute_is_there(templates) -> None:
    import uuid

    result, _ = run([env_ip(templates)])
    uuid.UUID(env_json(result)["customId"]["id"])  # raises if it is not a UUID


def test_the_crop_id_is_written_into_the_rotation_slot(templates) -> None:
    result, _ = run([env_ip(templates, setup=setup_message(templates, cropId="SM"))])
    assert env_json(result)["cropRotation"][2] == "SM"


def test_latitude_is_taken_from_the_coordinate_when_the_setup_says_so(templates) -> None:
    setup = setup_message(templates, latitude=True)
    result, _ = run([env_ip(templates, lat=48.5, setup=setup)])
    assert env_json(result)["params"]["siteParameters"]["Latitude"] == 48.5


def test_latitude_is_left_alone_when_the_setup_does_not_ask(templates) -> None:
    result, _ = run([env_ip(templates, setup=setup_message(templates, latitude=False))])
    assert env_json(result)["params"]["siteParameters"]["Latitude"] == 1.0


def test_elevation_comes_from_a_grid_value_attribute(templates) -> None:
    setup = setup_message(templates, elevation=True)
    dgm = grid_capnp.Grid.Value.new_message(f=123.5)
    result, _ = run([env_ip(templates, setup=setup, dgm=dgm)])
    assert env_json(result)["params"]["siteParameters"]["heightNN"] == 123.5


def test_slope_is_converted_from_percent(templates) -> None:
    setup = setup_message(templates, slope=True)
    slope = grid_capnp.Grid.Value.new_message(f=25.0)
    result, _ = run([env_ip(templates, setup=setup, slope=slope)])
    assert env_json(result)["params"]["siteParameters"]["slope"] == 0.25


def test_co2_and_o3_are_applied_when_positive(templates) -> None:
    setup = setup_message(templates, co2=450.0, o3=30.0)
    result, _ = run([env_ip(templates, setup=setup)])
    env_params = env_json(result)["params"]["userEnvironmentParameters"]
    assert env_params["AtmosphericCO2"] == pytest.approx(450.0)
    assert env_params["AtmosphericO3"] == pytest.approx(30.0)


def test_co2_is_left_alone_when_zero(templates) -> None:
    result, _ = run([env_ip(templates)])
    assert env_json(result)["params"]["userEnvironmentParameters"]["AtmosphericCO2"] == 380.0


def test_ilr_dates_overwrite_the_sowing_and_harvest_worksteps(templates) -> None:
    ilr = mgmt_capnp.ILRDates.new_message(
        sowing={"year": 2021, "month": 4, "day": 15},
        harvest={"year": 2021, "month": 8, "day": 20},
    )
    result, _ = run([env_ip(templates, ilr=ilr)])
    worksteps = env_json(result)["cropRotation"][0]["worksteps"]
    assert worksteps[0]["date"] == "2021-04-15"
    assert worksteps[1]["date"] == "2021-08-20"


def test_only_the_ilr_dates_that_are_set_are_applied(templates) -> None:
    ilr = mgmt_capnp.ILRDates.new_message(sowing={"year": 2021, "month": 4, "day": 15})
    result, _ = run([env_ip(templates, ilr=ilr)])
    worksteps = env_json(result)["cropRotation"][0]["worksteps"]
    assert worksteps[0]["date"] == "2021-04-15"
    assert worksteps[1]["date"] == "2020-09-01"


def test_simulation_flags_come_from_the_setup(templates) -> None:
    setup = setup_message(templates, fertilization=True, irrigation=True, nitrogenResponseOn=True)
    result, _ = run([env_ip(templates, setup=setup)])
    sim_params = env_json(result)["params"]["simulationParameters"]
    assert sim_params["UseNMinMineralFertilisingMethod"] is True
    assert sim_params["UseAutomaticIrrigation"] is True
    assert sim_params["NitrogenResponseOn"] is True


def test_stage_temperature_sum_is_applied_when_the_length_matches(templates) -> None:
    setup = setup_message(templates, stageTemperatureSum="10_20_30")
    result, _ = run([env_ip(templates, setup=setup)])
    cultivar = env_json(result)["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"]
    assert cultivar["StageTemperatureSum"][0] == [10, 20, 30]


def test_a_stage_temperature_sum_of_the_wrong_length_is_ignored(templates) -> None:
    setup = setup_message(templates, stageTemperatureSum="10_20")
    result, _ = run([env_ip(templates, setup=setup)])
    cultivar = env_json(result)["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"]
    assert cultivar["StageTemperatureSum"][0] == [100, 200, 300]


def test_a_non_numeric_stage_temperature_sum_does_not_lose_the_ip(templates) -> None:
    setup = setup_message(templates, stageTemperatureSum="a_b_c")
    result, _ = run([env_ip(templates, setup=setup)])
    assert len(standard(result.output("out"))) == 1


def test_a_climate_csv_path_is_inlined(templates) -> None:
    result, _ = run([env_ip(templates, climate="/data/climate.csv")])
    assert env_json(result)["pathToClimateCSV"] == "/data/climate.csv"


def test_templates_are_loaded_once_and_reused(templates) -> None:
    _, component = run([env_ip(templates), env_ip(templates), env_ip(templates)])
    assert component.templates.loads == 1


def test_each_env_gets_its_own_copy_of_the_template(templates) -> None:
    """The cache used to hand out the same dict every time, so the second IP inherited the
    first one's elevation, slope and dates."""

    setup = setup_message(templates, elevation=True)
    first = env_ip(templates, setup=setup, dgm=grid_capnp.Grid.Value.new_message(f=100.0))
    second = env_ip(templates, setup=setup)

    result, _ = run([first, second])
    assert env_json(result, 0)["params"]["siteParameters"]["heightNN"] == 100.0
    assert env_json(result, 1)["params"]["siteParameters"]["heightNN"] == 0.0


def test_brackets_pass_through(templates) -> None:
    result, _ = run([open_bracket_message(), env_ip(templates), close_bracket_message()])
    assert [str(v.type) for v in result.output("out").values] == [
        "openBracket",
        "standard",
        "closeBracket",
    ]


def test_an_ip_without_a_coordinate_is_skipped_by_default(templates) -> None:
    ip = fbp_capnp.IP.new_message()
    entries = ip.init("attributes", 1)
    entries[0].key = "setup"
    entries[0].value = setup_message(templates)

    result, _ = run([PortMessage(PortValue(ip)), env_ip(templates)])
    assert len(standard(result.output("out"))) == 1


def test_an_ip_without_a_setup_can_fail_the_process(templates) -> None:
    ip = fbp_capnp.IP.new_message()
    entries = ip.init("attributes", 1)
    entries[0].key = "latlon"
    entries[0].value = geo_capnp.LatLonCoord.new_message(lat=52.0, lon=13.0)

    with pytest.raises(ValueError, match="setup"):
        run([PortMessage(PortValue(ip))], on_error="fail")


def test_the_attribute_names_are_configurable(templates) -> None:
    result, _ = run([env_ip(templates)], coord="@latlon", setup="@setup")
    assert len(standard(result.output("out"))) == 1


def test_several_ips_in_a_row(templates) -> None:
    result, _ = run([env_ip(templates), env_ip(templates)])
    assert len(standard(result.output("out"))) == 2
