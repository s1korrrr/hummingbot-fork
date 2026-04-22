from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.mock.mock_cli import CLIMockingAssistant
from typing import Union
from unittest.mock import patch

from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.config.config_helpers import ClientConfigAdapter, read_system_configs_from_yml
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.strategy_config_data_types import BaseStrategyConfigMap
from hummingbot.client.hummingbot_application import HummingbotApplication


class ConfigCommandTest(IsolatedAsyncioWrapperTestCase):
    @patch("hummingbot.core.utils.trading_pair_fetcher.TradingPairFetcher")
    @patch("hummingbot.core.gateway.gateway_http_client.GatewayHttpClient.start_monitor")
    @patch("hummingbot.client.hummingbot_application.HummingbotApplication.mqtt_start")
    async def asyncSetUp(self, mock_mqtt_start, mock_gateway_start, mock_trading_pair_fetcher):
        await read_system_configs_from_yml()
        self.app = HummingbotApplication()
        self.cli_mock_assistant = CLIMockingAssistant(self.app.app)
        self.cli_mock_assistant.start()

    def tearDown(self) -> None:
        self.cli_mock_assistant.stop()
        super().tearDown()

    @patch("hummingbot.client.hummingbot_application.get_strategy_config_map")
    @patch("hummingbot.client.hummingbot_application.HummingbotApplication.notify")
    def test_list_configs(self, notify_mock, get_strategy_config_map_mock):
        captures = []
        self.app.client_config_map.instance_id = "TEST_ID"
        notify_mock.side_effect = lambda s: captures.append(s)
        strategy_name = "some-strategy"
        self.app.trading_core.strategy_name = strategy_name
        self.app.client_config_map.commands_timeout.other_commands_timeout = Decimal("30.0")

        strategy_config_map_mock = {
            "five": ConfigVar(key="five", prompt=""),
            "six": ConfigVar(key="six", prompt="", default="sixth"),
        }
        strategy_config_map_mock["five"].value = "fifth"
        strategy_config_map_mock["six"].value = "sixth"
        get_strategy_config_map_mock.return_value = strategy_config_map_mock

        self.app.list_configs()

        self.assertEqual(6, len(captures))
        self.assertEqual("\nGlobal Configurations:", captures[0])
        global_config_table = captures[1]
        self.assertIn("| instance_id                       | TEST_ID", global_config_table)
        self.assertIn("| fetch_pairs_from_all_exchanges    | False", global_config_table)
        self.assertIn("| ∟ mqtt_host                       | localhost", global_config_table)
        self.assertIn("| telegram_notifications            |", global_config_table)
        self.assertIn(
            f"| ∟ telegram_enabled                | {self.app.client_config_map.telegram_notifications.telegram_enabled}",
            global_config_table,
        )
        self.assertIn(
            f"| ∟ telegram_chat_id                | {self.app.client_config_map.telegram_notifications.telegram_chat_id}",
            global_config_table,
        )
        self.assertIn("| ∟ telegram_notify_order_filled    | True", global_config_table)
        self.assertIn("| ∟ telegram_notify_order_completed | False", global_config_table)
        self.assertIn("| ∟ telegram_notify_order_failed    | True", global_config_table)
        self.assertIn("| ∟ gateway_api_host                | localhost", global_config_table)
        self.assertIn("| ∟ global_token_name               | USDT", global_config_table)
        self.assertIn("| tables_format                     | psql", global_config_table)
        self.assertIn("| ∟ market_data_collection_depth    | 20", global_config_table)
        self.assertEqual("\nColor Settings:", captures[2])

        df_str_expected = ("    +--------------------+---------+\n"
                           "    | Key                | Value   |\n"
                           "    |--------------------+---------|\n"
                           "    | ∟ top_pane         | #000000 |\n"
                           "    | ∟ bottom_pane      | #000000 |\n"
                           "    | ∟ output_pane      | #262626 |\n"
                           "    | ∟ input_pane       | #1C1C1C |\n"
                           "    | ∟ logs_pane        | #121212 |\n"
                           "    | ∟ terminal_primary | #5FFFD7 |\n"
                           "    +--------------------+---------+")

        self.assertEqual(df_str_expected, captures[3])
        self.assertEqual("\nStrategy Configurations:", captures[4])

        df_str_expected = (
            "    +-------+---------+"
            "\n    | Key   | Value   |"
            "\n    |-------+---------|"
            "\n    | five  | fifth   |"
            "\n    | six   | sixth   |"
            "\n    +-------+---------+"
        )

        self.assertEqual(df_str_expected, captures[5])

    @patch("hummingbot.client.hummingbot_application.get_strategy_config_map")
    @patch("hummingbot.client.hummingbot_application.HummingbotApplication.notify")
    def test_list_configs_pydantic_model(self, notify_mock, get_strategy_config_map_mock):
        captures = []
        notify_mock.side_effect = lambda s: captures.append(s)
        strategy_name = "some-strategy"
        self.app.trading_core.strategy_name = strategy_name

        class DoubleNestedModel(BaseClientModel):
            double_nested_attr: float = Field(default=3.0)

            class Config:
                title = "double_nested_model"

        class NestedModelOne(BaseClientModel):
            nested_attr: str = Field(default="some value")
            double_nested_model: DoubleNestedModel = Field(default=DoubleNestedModel())

            class Config:
                title = "nested_mode_one"

        class NestedModelTwo(BaseClientModel):
            class Config:
                title = "nested_mode_two"

        class DummyModel(BaseClientModel):
            some_attr: int = Field(default=1)
            nested_model: Union[NestedModelTwo, NestedModelOne] = Field(default=NestedModelOne())
            another_attr: Decimal = Field(default=Decimal("1.0"))
            missing_no_default: int = Field(default=...)

            class Config:
                title = "dummy_model"

        get_strategy_config_map_mock.return_value = ClientConfigAdapter(DummyModel.model_construct())

        self.app.list_configs()

        self.assertEqual(6, len(captures))

        self.assertEqual("\nStrategy Configurations:", captures[4])

        df_str_expected = (
            "    +------------------------+------------------------+"
            "\n    | Key                    | Value                  |"
            "\n    |------------------------+------------------------|"
            "\n    | some_attr              | 1                      |"
            "\n    | nested_model           | nested_mode_one        |"
            "\n    | ∟ nested_attr          | some value             |"
            "\n    | ∟ double_nested_model  |                        |"
            "\n    |   ∟ double_nested_attr | 3.0                    |"
            "\n    | another_attr           | 1.0                    |"
            "\n    | missing_no_default     | &cMISSING_AND_REQUIRED |"
            "\n    +------------------------+------------------------+"
        )

        self.assertEqual(df_str_expected, captures[5])

    @patch("hummingbot.client.hummingbot_application.get_strategy_config_map")
    @patch("hummingbot.client.hummingbot_application.HummingbotApplication.notify")
    def test_config_non_configurable_key_fails(self, notify_mock, get_strategy_config_map_mock):
        class DummyModel(BaseStrategyConfigMap):
            strategy: str = Field(default="pure_market_making")
            some_attr: int = Field(default=1, json_schema_extra={"prompt": "some prompt"})
            another_attr: Decimal = Field(default=Decimal("1.0"))

            class Config:
                title = "dummy_model"

        strategy_name = "some-strategy"
        self.app.trading_core.strategy_name = strategy_name
        get_strategy_config_map_mock.return_value = ClientConfigAdapter(DummyModel.model_construct())
        self.app.config(key="some_attr")

        notify_mock.assert_not_called()

        self.app.config(key="another_attr")

        notify_mock.assert_called_once_with("Invalid key, please choose from the list.")

        notify_mock.reset_mock()
        self.app.config(key="some_key")

        notify_mock.assert_called_once_with("Invalid key, please choose from the list.")

    @patch("hummingbot.client.command.config_command.save_to_yml")
    @patch("hummingbot.client.hummingbot_application.get_strategy_config_map")
    @patch("hummingbot.client.hummingbot_application.HummingbotApplication.notify")
    async def test_config_single_keys(self, _, get_strategy_config_map_mock, save_to_yml_mock):
        class NestedModel(BaseClientModel):
            nested_attr: str = Field(default="some value", json_schema_extra={"prompt": "some prompt"})

            class Config:
                title = "nested_model"

        class DummyModel(BaseStrategyConfigMap):
            strategy: str = Field(default="pure_market_making")
            some_attr: int = Field(default=1, json_schema_extra={"prompt": "some prompt"})
            nested_model: NestedModel = Field(default=NestedModel())

            class Config:
                title = "dummy_model"

        strategy_name = "some-strategy"
        self.app.trading_core.strategy_name = strategy_name
        self.app.strategy_file_name = f"{strategy_name}.yml"
        config_map = ClientConfigAdapter(DummyModel.model_construct())
        get_strategy_config_map_mock.return_value = config_map

        await self.app._config_single_key(key="some_attr", input_value=2)

        self.assertEqual(2, config_map.some_attr)
        save_to_yml_mock.assert_called_once()

        save_to_yml_mock.reset_mock()
        self.cli_mock_assistant.queue_prompt_reply("3")
        await self.app._config_single_key(key="some_attr", input_value=None)

        self.assertEqual(3, config_map.some_attr)
        save_to_yml_mock.assert_called_once()

        save_to_yml_mock.reset_mock()
        self.cli_mock_assistant.queue_prompt_reply("another value")
        await self.app._config_single_key(key="nested_model.nested_attr", input_value=None)

        self.assertEqual("another value", config_map.nested_model.nested_attr)
        save_to_yml_mock.assert_called_once()
