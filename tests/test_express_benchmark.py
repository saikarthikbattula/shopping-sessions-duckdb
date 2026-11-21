"""Candidate promotion requires exact outputs and consistent measured gains."""
import copy
import unittest
from scripts.benchmark_express_lane import recommend


class CandidateSelectionTests(unittest.TestCase):
    def report(self):
        return dict(exact_outputs=True,fast_path={'stock':True,'spin0':True,'spin50':True},
                    summary={name:dict(wall_seconds=wall,cpu_seconds=cpu) for name,wall,cpu in
                             [('trusted',2,4),('stock',1,3),('spin0',.8,2),('spin50',1.1,3.1)]})

    def test_tuning_requires_both_seeds_and_cpu_gain(self):
        a,b=self.report(),self.report()
        self.assertEqual(recommend([a,b])['selected'],'spin0')
        b['summary']['spin0']['cpu_seconds']=3.2
        self.assertEqual(recommend([a,b])['selected'],'stock')
        self.assertIsNone(recommend([a])['selected'])

    def test_fallback_or_different_outputs_prevent_promotion(self):
        a,b=self.report(),self.report()
        b['exact_outputs']=False
        self.assertIsNone(recommend([a,b])['selected'])
        b=self.report()
        for name in b['fast_path']: b['fast_path'][name]=False
        self.assertIsNone(recommend([a,b])['selected'])
