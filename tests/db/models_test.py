

import unittest
from cn_stats_data.db.models import MetricCodeDownloadCheckpoint, Metric

class TestMetricCodeDownloadCheckpoint(unittest.TestCase):

    def setUp(self):
        self.checkpoint = MetricCodeDownloadCheckpoint(db_code='test_db', metric_code='test_metric')

    def test_initial_state(self):
        self.assertEqual(self.checkpoint.db_code, 'test_db')
        self.assertEqual(self.checkpoint.metric_code, 'test_metric')
        self.assertFalse(self.checkpoint._db_checkpoint_located)
        self.assertFalse(self.checkpoint._metric_checkpoint_located)

    def test_reset_if_parameters_changed(self):
        self.checkpoint.reset_if_parameters_changed(db_code='new_db', metric_code='new_metric')
        self.assertEqual(self.checkpoint.db_code, 'new_db')
        self.assertEqual(self.checkpoint.metric_code, 'new_metric')
        self.assertFalse(self.checkpoint._db_checkpoint_located)
        self.assertFalse(self.checkpoint._metric_checkpoint_located)

    def test_need_skip_db(self):
        self.checkpoint.db_checkpoint = 'test_db'
        self.assertTrue(self.checkpoint.need_skip_db('other_db'))
        self.assertFalse(self.checkpoint.need_skip_db('test_db'))
        self.assertFalse(self.checkpoint.need_skip_db('other_db'))

    def test_need_skip_metric(self):

        self.checkpoint.metric_checkpoint = ['test_metric_1', 'test_metric_2']
        metric = Metric.of(db_code='test_db', code='test_metric')
        self.assertTrue(self.checkpoint.need_skip_metric(metric))

        metric1 = Metric.of(db_code='test_db', code='test_metric_1')
        self.assertFalse(self.checkpoint.need_skip_metric(metric1))

        self.assertTrue(self.checkpoint.need_skip_metric(metric))
        self.assertEqual(self.checkpoint.metric_checkpoint, ['test_metric_1', 'test_metric_2'])

        metric2 = Metric.of(db_code='test_db', code='test_metric_2')
        metric2.parent = metric1
        self.assertFalse(self.checkpoint.need_skip_metric(metric2))
        
        self.assertFalse(self.checkpoint.need_skip_metric(metric))
        self.assertEqual(self.checkpoint.metric_checkpoint, ['test_metric'])

    def test_reset_checkpoint(self):
        self.checkpoint.reset_checkpoint()
        self.assertIsNone(self.checkpoint.db_code)
        self.assertIsNone(self.checkpoint.metric_code)
        self.assertFalse(self.checkpoint._db_checkpoint_located)
        self.assertFalse(self.checkpoint._metric_checkpoint_located)

if __name__ == '__main__':
    unittest.main()