from dagster import RepositoryDefinition, pipeline, solid
from dagster.core.definitions.container import get_active_repository_data_from_image
from dagster.core.snap import active_repository_data_from_def
from dagster.serdes import serialize_dagster_namedtuple


def noop_repo():
    @solid
    def noop_solid(_):
        return 1

    @pipeline
    def noop_pipeline():
        noop_solid()

    return RepositoryDefinition(name='noop_repo', pipeline_defs=[noop_pipeline])


# pylint: disable=unused-argument
def mock_active_repository_data():
    active_repo_data = active_repository_data_from_def(noop_repo())
    return serialize_dagster_namedtuple(active_repo_data)


def test_container_snapshot_provider(mocker):

    execute_container_mock = mocker.patch(
        'dagster.core.definitions.container.run_serialized_container_command',
        return_value=[mock_active_repository_data()],
    )
    active_repository_data = get_active_repository_data_from_image("foo:latest")
    execute_container_mock.assert_called_with(
        image="foo:latest", command='dagster api snapshot repository', volumes=mocker.ANY,
    )
    assert active_repository_data == active_repository_data_from_def(noop_repo())
