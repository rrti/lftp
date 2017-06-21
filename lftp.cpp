#include "lftp.hpp"

#ifndef DISABLE_THREADPOOL

#define USE_TASK_STATS_TRACKING
#define USE_BOOST_LOCKFREE_QUEUE

#define BASIC_TASK_QUEUE_IDX 0
#define ASYNC_TASK_QUEUE_IDX 1

#ifdef USE_BOOST_LOCKFREE_QUEUE
#include <boost/lockfree/queue.hpp>
#else
#include "concurrent_queue.h"
#endif



typedef std::thread t_thread;

namespace util {
	template<typename type> type clamp(type v, type a, type b) { return (std::max(a, std::min(b, v))); }

	// missing in C++11, roll our own
	template<typename t_type, typename... t_args>
	std::unique_ptr<t_type> make_unique(t_args&&... args) {
		return (std::unique_ptr<t_type>(new t_type(std::forward<t_args>(args)...)));
	}

	struct t_thread_stats {
		uint64_t num_tasks_run;
		uint64_t sum_exec_time;
		uint64_t min_exec_time;
		uint64_t max_exec_time;
		uint64_t sum_wait_time;
		uint64_t min_wait_time;
		uint64_t max_wait_time;
	};
};

namespace threading {
	// stubs
	struct t_signal {
		void wait_for(const t_time&) {}
		void notify_all(uint32_t) {}
	};

	uint32_t get_physical_cpu_cores() { return 0; }
	uint32_t get_logical_cpu_cores() { return (std::thread::hardware_concurrency()); }

	void set_thread_name(const char*) {}
};



std::atomic<uint32_t> t_base_task_group::last_id(0);

// note: std::shared_ptr<T> can not be made atomic, queues must store T*'s
#ifdef USE_BOOST_LOCKFREE_QUEUE
static std::array<boost::lockfree::queue<t_base_task_group*>, thread_pool::MAX_THREADS> task_queues[2];
#else
static std::array<moodycamel::ConcurrentQueue<t_base_task_group*>, thread_pool::MAX_THREADS> task_queues[2];
#endif

static std::vector< std::unique_ptr<t_thread> > pool_threads[2];
static std::array<bool, thread_pool::MAX_THREADS> exit_flags;
static std::array<util::t_thread_stats, thread_pool::MAX_THREADS> thread_stats[2];
static std::array<char[64], thread_pool::MAX_THREADS> thread_names[2];
static threading::t_signal new_tasks_signals[2];

static __thread uint32_t thread_num(0);



namespace thread_pool {
	uint32_t get_thread_num() { return thread_num; }
	static void set_thread_num(uint32_t idx) { thread_num = idx; }

	// NOTE: workers start at 1, we also count the main thread
	uint32_t get_num_threads() { return (pool_threads[BASIC_TASK_QUEUE_IDX].size() + 1); }
	uint32_t get_max_threads() { return std::min(thread_pool::MAX_THREADS, threading::get_logical_cpu_cores()); }

	bool has_threads() { return !pool_threads[BASIC_TASK_QUEUE_IDX].empty(); }



	static bool exec_tasks(uint32_t thread_id, uint32_t queue_idx) {
		t_base_task_group* tg = nullptr;

		// any external thread calling wait_for_finished will have
		// id = 0 and *only* processes tasks from the global queue
		for (uint32_t thread_queue_idx = 0; thread_queue_idx <= thread_id; thread_queue_idx += std::max(thread_id, 1u)) {
			auto& queue = task_queues[queue_idx][thread_queue_idx];

			#ifdef USE_BOOST_LOCKFREE_QUEUE
			if (queue.pop(tg)) {
			#else
			if (queue.try_dequeue(tg)) {
			#endif
				// inform other workers when there is global work to do
				// waking is an expensive kernel call, so better shift this
				// cost to the workers (the main thread only wakes when all
				// workers are sleeping)
				if (thread_queue_idx == 0)
					notify_threads(1, queue_idx);

				assert(queue_idx == ASYNC_TASK_QUEUE_IDX || tg->is_async_task());

				#ifdef USE_TASK_STATS_TRACKING
				const uint64_t wdt = tg->get_dt(t_time::now());
				const uint64_t edt = tg->execute_loop(thread_id, false);

				thread_stats[queue_idx][thread_id].num_tasks_run += 1;
				thread_stats[queue_idx][thread_id].sum_exec_time += edt;
				thread_stats[queue_idx][thread_id].sum_wait_time += wdt;
				thread_stats[queue_idx][thread_id].min_exec_time  = std::min(thread_stats[queue_idx][thread_id].min_exec_time, edt);
				thread_stats[queue_idx][thread_id].max_exec_time  = std::max(thread_stats[queue_idx][thread_id].max_exec_time, edt);
				thread_stats[queue_idx][thread_id].min_wait_time  = std::min(thread_stats[queue_idx][thread_id].min_wait_time, wdt);
				thread_stats[queue_idx][thread_id].max_wait_time  = std::max(thread_stats[queue_idx][thread_id].max_wait_time, wdt);
				#else
				tg->execute_loop(thread_id, false);
				#endif
			}

			#ifdef USE_BOOST_LOCKFREE_QUEUE
			while (queue.pop(tg)) {
			#else
			while (queue.try_dequeue(tg)) {
			#endif
				assert(queue_idx == ASYNC_TASK_QUEUE_IDX || tg->is_async_task());

				#ifdef USE_TASK_STATS_TRACKING
				const uint64_t wdt = tg->get_dt(t_time::now());
				const uint64_t edt = tg->execute_loop(thread_id, false);

				thread_stats[queue_idx][thread_id].num_tasks_run += 1;
				thread_stats[queue_idx][thread_id].sum_exec_time += edt;
				thread_stats[queue_idx][thread_id].sum_wait_time += wdt;
				thread_stats[queue_idx][thread_id].min_exec_time  = std::min(thread_stats[queue_idx][thread_id].min_exec_time, edt);
				thread_stats[queue_idx][thread_id].max_exec_time  = std::max(thread_stats[queue_idx][thread_id].max_exec_time, edt);
				thread_stats[queue_idx][thread_id].min_wait_time  = std::min(thread_stats[queue_idx][thread_id].min_wait_time, wdt);
				thread_stats[queue_idx][thread_id].max_wait_time  = std::max(thread_stats[queue_idx][thread_id].max_wait_time, wdt);
				#else
				tg->execute_loop(thread_id, false);
				#endif
			}
		}

		// if true, queue contained at least one element
		return (tg != nullptr);
	}


	static void worker_loop(uint32_t thread_id, uint32_t queue_idx) {
		assert(thread_id != 0);
		set_thread_num(thread_id);

		memset(thread_names[queue_idx][thread_id], 0, sizeof(thread_names[queue_idx][thread_id]));
		snprintf(thread_names[queue_idx][thread_id], sizeof(thread_names[queue_idx][thread_id]), "worker%i", thread_id);
		threading::set_thread_name(thread_names[queue_idx][thread_id]);

		// make first worker spin a while before sleeping/waiting on the thread signal
		// this increases the chance that at least one worker is awake when a new task
		// is inserted, which can then take over the job of waking up sleeping workers
		// (see notify_threads)
		// NOTE: the spin-time has to be *short* to avoid biasing thread 1's workload
		const t_time our_spin_time = t_time::from_us(30 * (thread_id == 1));
		const t_time max_wait_time = t_time::from_ms(30);

		while (!exit_flags[thread_id]) {
			const t_time spin_end_time = t_time::now() + our_spin_time;
				  t_time cur_wait_time = t_time::from_us(1);

			while (!exec_tasks(thread_id, queue_idx) && !exit_flags[thread_id]) {
				if (t_time::now() < spin_end_time)
					continue;

				new_tasks_signals[queue_idx].wait_for(cur_wait_time = std::min(cur_wait_time * 1.25f, max_wait_time));
			}
		}
	}





	void wait_for_finished(std::shared_ptr<t_base_task_group>&& task_group) {
		// can be any worker-thread (for_mt inside another for_mt, etc)
		const uint32_t thread_id = get_thread_num();

		{
			assert(!task_group->is_async_task());
			assert(!task_group->self_delete());
			task_group->execute_loop(thread_id, true);
		}

		// NOTE:
		//   it is possible for the task-group to have been completed
		//   entirely by the loop above, before any worker thread has
		//   even had a chance to pop it from the queue (so returning
		//   under that condition could cause the group to be deleted
		//   or reassigned prematurely) --> wait
		if (task_group->is_finished()) {
			while (!task_group->is_in_queue()) {
				exec_tasks(thread_id, BASIC_TASK_QUEUE_IDX);
			}

			task_group->reset_state(false, task_group->is_in_pool(), false);
			return;
		}


		// task hasn't completed yet, use waiting time to execute other tasks
		notify_threads(1, 0);

		do {
			const auto spinlock_end_time = t_time::now() + t_time::from_ms(500);

			while (!exec_tasks(thread_id, BASIC_TASK_QUEUE_IDX) && !task_group->is_finished() && !exit_flags[thread_id]) {
				if (t_time::now() < spinlock_end_time)
					continue;

				// avoid a hang if the task is still not finished
				notify_threads(1, 0);
				break;
			}
		} while (!task_group->is_finished() && !exit_flags[thread_id]);

		while (task_group->is_in_queue()) {
			exec_tasks(thread_id, BASIC_TASK_QUEUE_IDX);
		}

		task_group->reset_state(false, task_group->is_in_pool(), false);
	}


	// WARNING:
	//   leaking the raw pointer *forces* caller to wait_for_finished
	//   otherwise task might get deleted while its pointer is still
	//   in the queue
	void push_task_group(std::shared_ptr<t_base_task_group>&& task_group) { push_task_group(task_group.get()); }
	void push_task_group(t_base_task_group* task_group) {
		auto& queue = task_queues[ task_group->is_async_task() ][ task_group->wanted_thread() ];

		task_group->set_ts(t_time::now());

		#ifdef USE_BOOST_LOCKFREE_QUEUE
		while (!queue.push(task_group));
		#else
		while (!queue.enqueue(task_group));
		#endif

		// async_task's do not care about wakeup-latency as much
		if (task_group->is_async_task())
			return;

		notify_threads(0, 0);
	}

	void notify_threads(uint32_t force, uint32_t queue_idx) {
		// if !force, then only wake up threads when all are sleeping
		// this is an optimization: waking up other threads is a kernel
		// syscall that takes a lot of time (up to 1000ms) so we prefer
		// not to block the thread that called push_task_group, and let
		// the worker threads themselves inform each other
		new_tasks_signals[queue_idx].notify_all((get_num_threads() - 1) * (1 - force));
	}




	static void spawn_threads(uint32_t wanted_num_threads, uint32_t cur_num_threads) {
		for (uint32_t i = cur_num_threads; i < wanted_num_threads; ++i) {
			exit_flags[i] = false;

			pool_threads[BASIC_TASK_QUEUE_IDX].push_back(util::make_unique<t_thread>(std::bind(&worker_loop, i, BASIC_TASK_QUEUE_IDX)));
			pool_threads[ASYNC_TASK_QUEUE_IDX].push_back(util::make_unique<t_thread>(std::bind(&worker_loop, i, ASYNC_TASK_QUEUE_IDX)));
		}
	}

	static void kill_threads(uint32_t wanted_num_threads, uint32_t cur_num_threads) {
		for (uint32_t i = cur_num_threads - 1; i >= wanted_num_threads && i > 0; --i) {
			exit_flags[i] = true;
		}

		notify_threads(1, 0);

		for (uint32_t i = cur_num_threads - 1; i >= wanted_num_threads && i > 0; --i) {
			assert(!pool_threads[BASIC_TASK_QUEUE_IDX].empty());
			assert(!pool_threads[ASYNC_TASK_QUEUE_IDX].empty());

			(pool_threads[BASIC_TASK_QUEUE_IDX].back())->join();
			(pool_threads[ASYNC_TASK_QUEUE_IDX].back())->join();

			pool_threads[BASIC_TASK_QUEUE_IDX].pop_back();
			pool_threads[ASYNC_TASK_QUEUE_IDX].pop_back();
		}

		// play it safe
		for (uint32_t i = cur_num_threads - 1; i >= wanted_num_threads && i > 0; --i) {
			t_base_task_group* tg = nullptr;

			#ifdef USE_BOOST_LOCKFREE_QUEUE
			while (task_queues[BASIC_TASK_QUEUE_IDX][i].pop(tg));
			while (task_queues[ASYNC_TASK_QUEUE_IDX][i].pop(tg));
			#else
			while (task_queues[BASIC_TASK_QUEUE_IDX][i].try_dequeue(tg));
			while (task_queues[ASYNC_TASK_QUEUE_IDX][i].try_dequeue(tg));
			#endif
		}

		assert((wanted_num_threads != 0) || pool_threads[BASIC_TASK_QUEUE_IDX].empty());
	}




	void set_thread_count(uint32_t wanted_num_threads) {
		const uint32_t cur_num_threads = get_num_threads();
		const uint32_t wtd_num_threads = util::clamp(wanted_num_threads, 1u, get_max_threads());

		const char* fmts[4] = {
			"[thread_pool::%s][1] wanted=%d current=%d maximum=%d",
			"[thread_pool::%s][2] workers=%lu",
			"\t[async=%d] threads=%d tasks=%lu {sum,avg}{exec,wait}time={{%.3f, %.3f}, {%.3f, %.3f}}ms",
			"\t\tthread=%d tasks=%lu (%3.3f%%) {sum,min,max,avg}{exec,wait}time={{%.3f, %.3f, %.3f, %.3f}, {%.3f, %.3f, %.3f, %.3f}}ms",
		};

		// total number of tasks executed by pool; total time spent in exec_tasks
		uint64_t p_num_tasks_run[2] = {0lu, 0lu};
		uint64_t p_sum_exec_time[2] = {0lu, 0lu};
		uint64_t p_sum_wait_time[2] = {0lu, 0lu};

		printf(fmts[0], __func__, wanted_num_threads, cur_num_threads, get_max_threads());

		if (pool_threads[BASIC_TASK_QUEUE_IDX].empty()) {
			assert(pool_threads[ASYNC_TASK_QUEUE_IDX].empty());

			#ifdef USE_BOOST_LOCKFREE_QUEUE
			task_queues[BASIC_TASK_QUEUE_IDX][0].reserve(1024);
			task_queues[ASYNC_TASK_QUEUE_IDX][0].reserve(1024);
			#endif

			#ifdef USE_TASK_STATS_TRACKING
			for (uint32_t queue_idx: {BASIC_TASK_QUEUE_IDX, ASYNC_TASK_QUEUE_IDX}) {
				for (uint32_t i = 0; i < thread_pool::MAX_THREADS; i++) {
					thread_stats[queue_idx][i].num_tasks_run =  0lu;
					thread_stats[queue_idx][i].sum_exec_time =  0lu;
					thread_stats[queue_idx][i].min_exec_time = -1lu;
					thread_stats[queue_idx][i].max_exec_time =  0lu;
					thread_stats[queue_idx][i].sum_wait_time =  0lu;
					thread_stats[queue_idx][i].min_wait_time = -1lu;
					thread_stats[queue_idx][i].max_wait_time =  0lu;
				}
			}
			#endif
		}

		if (cur_num_threads < wtd_num_threads) {
			spawn_threads(wtd_num_threads, cur_num_threads);
		} else {
			kill_threads(wtd_num_threads, cur_num_threads);
		}

		#ifdef USE_TASK_STATS_TRACKING
		if (pool_threads[BASIC_TASK_QUEUE_IDX].empty()) {
			assert(pool_threads[ASYNC_TASK_QUEUE_IDX].empty());

			for (uint32_t queue_idx: {BASIC_TASK_QUEUE_IDX, ASYNC_TASK_QUEUE_IDX}) {
				for (uint32_t i = 0; i < cur_num_threads; i++) {
					p_num_tasks_run[queue_idx] += thread_stats[queue_idx][i].num_tasks_run;
					p_sum_exec_time[queue_idx] += thread_stats[queue_idx][i].sum_exec_time;
					p_sum_wait_time[queue_idx] += thread_stats[queue_idx][i].sum_wait_time;
				}
			}

			for (uint32_t queue_idx: {BASIC_TASK_QUEUE_IDX, ASYNC_TASK_QUEUE_IDX}) {
				const float q_sum_exec_time =  p_sum_exec_time[queue_idx] * 1e-6f;
				const float q_sum_wait_time =  p_sum_wait_time[queue_idx] * 1e-6f;
				const float q_avg_exec_time = (p_sum_exec_time[queue_idx] * 1e-6f) / std::max(p_num_tasks_run[queue_idx], uint64_t(1));
				const float q_avg_wait_time = (p_sum_wait_time[queue_idx] * 1e-6f) / std::max(p_num_tasks_run[queue_idx], uint64_t(1));

				printf(fmts[2], queue_idx, cur_num_threads, p_num_tasks_run[queue_idx],  q_sum_exec_time, q_avg_exec_time,  q_sum_wait_time, q_avg_wait_time);

				for (uint32_t i = 0; i < cur_num_threads; i++) {
					const util::t_thread_stats& ts = thread_stats[queue_idx][i];

					if (ts.num_tasks_run == 0)
						continue;

					const float t_sum_exec_time = ts.sum_exec_time * 1e-6f; // ms
					const float t_sum_wait_time = ts.sum_wait_time * 1e-6f; // ms
					const float t_min_exec_time = ts.min_exec_time * 1e-6f; // ms
					const float t_min_wait_time = ts.min_wait_time * 1e-6f; // ms
					const float t_max_exec_time = ts.max_exec_time * 1e-6f; // ms
					const float t_max_wait_time = ts.max_wait_time * 1e-6f; // ms
					const float t_avg_exec_time = t_sum_exec_time / std::max(ts.num_tasks_run, uint64_t(1));
					const float t_avg_wait_time = t_sum_wait_time / std::max(ts.num_tasks_run, uint64_t(1));
					const float t_rel_exec_frac = (ts.num_tasks_run * 1e2f) / std::max(p_num_tasks_run[queue_idx], uint64_t(1));

					printf(fmts[3], i, ts.num_tasks_run, t_rel_exec_frac,  t_sum_exec_time, t_min_exec_time, t_max_exec_time, t_avg_exec_time,  t_sum_wait_time, t_min_wait_time, t_max_wait_time, t_avg_wait_time);
				}
			}
		}
		#endif

		printf(fmts[1], __func__, pool_threads[BASIC_TASK_QUEUE_IDX].size());
	}

	void set_max_thread_count() {
		if (pool_threads[BASIC_TASK_QUEUE_IDX].empty()) {
			pool_threads[BASIC_TASK_QUEUE_IDX].reserve(thread_pool::MAX_THREADS);
			pool_threads[ASYNC_TASK_QUEUE_IDX].reserve(thread_pool::MAX_THREADS);
		}

		set_thread_count(get_max_threads());
	}
}

#endif

