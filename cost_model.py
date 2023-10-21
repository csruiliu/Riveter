import psutil

from scipy.optimize import curve_fit


class ProcLatencyEstimator:
    def __init__(self,
                 rand_write_speed,
                 rand_read_speed,
                 num_join_array,
                 num_groupby_array,
                 input_cardinality_array,
                 suspension_point_array,
                 persistence_size_array):
        # rand r/w speed of hardware storage
        self.rand_write_speed = rand_write_speed
        self.rand_read_speed = rand_read_speed

        # the data for regression
        self.num_join_array = num_join_array
        self.num_groupby_array = num_groupby_array
        self.input_cardinality_array = input_cardinality_array
        self.suspension_point_array = suspension_point_array

        # the results/label for regression
        self.persistence_size_array = persistence_size_array

        # the param estimated from regression
        self.param = None

        # estimate latency
        self.suspend_latency_est = None
        self.resume_latency_est = None

    @staticmethod
    def func_persistence_size(X, a, b, c, d, e):
        (x1, x2, x3, x4) = X
        y = a * x1 + b * x2 + c * x3 + d * x4 + e
        return y.ravel()

    def fit_curve(self):
        assert len(self.num_join_array) == len(self.input_cardinality_array) == len(self.suspension_point_array)
        x = (self.num_join_array, self.num_groupby_array, self.input_cardinality_array, self.suspension_point_array)
        y = self.persistence_size_array

        self.param, _ = curve_fit(self.func_persistence_size, x, y)

    def persist_size_estimation(self, num_join, num_groupby, input_card, suspension_point):
        return (num_join * self.param[0] +
                num_groupby * self.param[1] +
                input_card * self.param[2] +
                suspension_point * self.param[3] + self.param[4])

    def suspend_latency_estimation(self, num_join, num_groupby, input_card, suspension_point):
        persist_size_est = self.persist_size_estimation(num_join, num_groupby, input_card, suspension_point)
        self.suspend_latency_est = persist_size_est / self.rand_write_speed
        return self.suspend_latency_est

    def resume_latency_estimation(self, num_join, num_groupby, input_card, suspension_point):
        persist_size_est = self.persist_size_estimation(num_join, num_groupby, input_card, suspension_point)
        self.resume_latency_est = persist_size_est / self.rand_read_speed
        return self.resume_latency_est


class PipelineLatencyEstimator:
    def __init__(self, persistence_size, rand_write_speed, rand_read_speed):
        self.persistence_size = persistence_size
        self.rand_write_speed = rand_write_speed
        self.rand_read_speed = rand_read_speed

        # estimate latency
        self.suspend_latency_est = None
        self.resume_latency_est = None

    def suspend_latency_estimation(self):
        self.suspend_latency_est = self.persistence_size / self.rand_write_speed
        return self.suspend_latency_est

    def resume_latency_estimation(self):
        self.resume_latency_est = self.persistence_size / self.rand_read_speed
        return self.resume_latency_est


def profile_hardware():
    hw_threads_num = psutil.cpu_count(logical=True)
    total_memory_gb = psutil.virtual_memory()[0] / 1000000000
    available_memory_gb = psutil.virtual_memory()[1] / 1000000000

    print(f"Number of Hardware Threads: {hw_threads_num}")
    print(f"Total Memory Size (GB): {total_memory_gb}")
    print(f"Available Memory Size (GB): {available_memory_gb}")

    return hw_threads_num, total_memory_gb, available_memory_gb

