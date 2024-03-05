# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import copy
import unittest

from CosmoTech_Acceleration_Library.Accelerators.scenario_download.scenario_downloader import get_content_from_twin_graph_data


class TestModelUtil(unittest.TestCase):
    maxDiff = None

    nodes = [{
        "n": {
            "id": "43",
            "label": "Customer",
            "properties": {
                "Satisfaction": 0,
                "SurroundingSatisfaction": 0,
                "Thirsty": False,
                "id": "Kyra_van_den_Hoek"
            },
            "type": "NODE",
        },
    }, {
        "n": {
            "id": "44",
            "label": "Customer",
            "properties": {
                "Satisfaction": 0,
                "SurroundingSatisfaction": 0,
                "Thirsty": False,
                "id": "Tyler_Post"
            },
            "type": "NODE",
        },
    }, {
        "n": {
            "id": "50",
            "label": "Customer",
            "properties": {
                "Satisfaction": 0,
                "SurroundingSatisfaction": 0,
                "Thirsty": False,
                "id": "Lars_Coret"
            },
            "type": "NODE",
        }
    }]

    edges = [
        {
            "dest": {
                "id": "43",
                "label": "Customer",
                "properties": {
                    "Satisfaction": 0,
                    "SurroundingSatisfaction": 0,
                    "Thirsty": False,
                    "id": "Kyra_van_den_Hoek",
                },
                "type": "NODE",
            },
            "rel": {
                "id": "175",
                "label": "arc_Satisfaction",
                "properties": {
                    "name": "arc_from_Lars_Coret_to_Kyra_van_den_Hoek",
                },
                "type": "RELATION",
            },
            "src": {
                "id": "50",
                "label": "Customer",
                "properties": {
                    "Satisfaction": 0,
                    "SurroundingSatisfaction": 0,
                    "Thirsty": False,
                    "id": "Lars_Coret",
                },
                "type": "NODE",
            },
        },
        {
            "dest": {
                "id": "44",
                "label": "Customer",
                "properties": {
                    "Satisfaction": 0,
                    "SurroundingSatisfaction": 0,
                    "Thirsty": False,
                    "id": "Tyler_Post",
                },
                "type": "NODE",
            },
            "rel": {
                "id": "179",
                "label": "arc_Satisfaction",
                "properties": {
                    "name": "arc_from_Lars_Coret_to_Tyler_Post",
                },
                "type": "RELATION",
            },
            "src": {
                "id": "50",
                "label": "Customer",
                "properties": {
                    "Satisfaction": 0,
                    "SurroundingSatisfaction": 0,
                    "Thirsty": False,
                    "id": "Lars_Coret",
                },
                "type": "NODE",
            },
        },
    ]

    expected_v2_twingraph_content = {
        "Customer": [
            {
                "Satisfaction": 0,
                "SurroundingSatisfaction": 0,
                "Thirsty": False,
                "id": "43",
            },
            {
                "Satisfaction": 0,
                "SurroundingSatisfaction": 0,
                "Thirsty": False,
                "id": "44",
            },
            {
                "Satisfaction": 0,
                "SurroundingSatisfaction": 0,
                "Thirsty": False,
                "id": "50",
            },
        ],
        "arc_Satisfaction": [{
            "id": "175",
            "name": "arc_from_Lars_Coret_to_Kyra_van_den_Hoek",
            "source": "50",
            "target": "43"
        }, {
            "id": "179",
            "name": "arc_from_Lars_Coret_to_Tyler_Post",
            "source": "50",
            "target": "44"
        }]
    }
    expected_v3_twingraph_content = {
        "Customer": [
            {
                "Satisfaction": 0,
                "SurroundingSatisfaction": 0,
                "Thirsty": False,
                "id": "Kyra_van_den_Hoek",
            },
            {
                "Satisfaction": 0,
                "SurroundingSatisfaction": 0,
                "Thirsty": False,
                "id": "Tyler_Post",
            },
            {
                "Satisfaction": 0,
                "SurroundingSatisfaction": 0,
                "Thirsty": False,
                "id": "Lars_Coret",
            },
        ],
        "arc_Satisfaction": [{
            "id": "175",
            "name": "arc_from_Lars_Coret_to_Kyra_van_den_Hoek",
            "source": "Lars_Coret",
            "target": "Kyra_van_den_Hoek"
        }, {
            "id": "179",
            "name": "arc_from_Lars_Coret_to_Tyler_Post",
            "source": "Lars_Coret",
            "target": "Tyler_Post"
        }]
    }

    def test_v2_twingraph_get_content(self):
        self.assertEqual(
            self.expected_v2_twingraph_content,
            get_content_from_twin_graph_data(copy.deepcopy(self.nodes), copy.deepcopy(self.edges)))

    def test_v3_twingraph_get_content(self):
        self.assertEqual(
            self.expected_v3_twingraph_content,
            get_content_from_twin_graph_data(copy.deepcopy(self.nodes), copy.deepcopy(self.edges), True))


if __name__ == '__main__':
    unittest.main()
