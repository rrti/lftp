#ifndef LOCKFREE_THREADPOOL_HDR
#define LOCKFREE_THREADPOOL_HDR


#ifdef DISABLE_THREADPOOL
#include <functional>

namespace thread_pool {
	template<class F, class... Args>
	static inline void enqueue(F&& f, Args&&... args) { f(args ...); }

	static inline void set_max_thread_count() {}
	static inline void set_thread_count() {}
	static inline void notify_threads(uint32_t, uint32_t) {}

	static inline uint32_t get_thread_num() { return 0; }
	static inline uint32_t get_num_threads() { return 1; }
	static inline uint32_t get_max_threads() { return 1; }

	static inline bool has_threads() { return false; }

	static constexpr uint32_t MAX_THREADS = 1;
};



static inline void for_mt(int32_t start, int32_t end, int32_t step, const std::function<void(const int32_t i)>&& f) {
	for (int32_t i = start; i < end; i += step) {
		f(i);
	}
}

static inline void for_mt(int32_t start, int32_t end, const std::function<void(const int32_t i)>&& f) {
	for_mt(start, end, 1, std::move(f));
}

static inline void for_mt2(int32_t start, int32_t end, unsigned work_size, const std::function<void(const int32_t i)>&& f) {
	(void) work_size; for_mt(start, end, 1, std::move(f));
}


static inline void parallel(const std::function<void()>&& f) { f(); }

template<class F, class G>
static inline auto parallel_reduce(F&& f, G&& g) -> typename std::result_of<F()>::type { return f(); }

#else
#include <cassert>

#include <array>
#include <vector>

#include <atomic>
#include <functional>
#include <future>
#include <numeric> // std::accumulate
#include <memory> // std::{unique,shared}_ptr



struct t_time {
	static t_time now() { return {}; }
	static t_time from_ms(uint64_t) { return {}; }
	static t_time from_us(uint64_t) { return {}; }
	uint64_t to_us() const { return 0; }
	uint64_t to_ns() const { return 0; }

	t_time operator + (const t_time&) const { return {}; }
	t_time operator - (const t_time&) const { return {}; }
	t_time operator * (float) const { return {}; }

	bool operator < (const t_time&) const { return false; }
	bool operator > (const t_time&) const { return false; }
};



class t_base_task_group {
public:
	t_base_task_group(const bool getid = true, const bool pooled = false): m_id(getid? last_id.fetch_add(1): -1u), m_ts(0) {
		reset_state(pooled);
	}

	virtual ~t_base_task_group() {
		// pooled tasks are deleted only when their pool dies (on exit)
		assert(is_finished() && (!is_in_queue() || is_in_pool()));
	}

	virtual bool is_async_task() const { return false; }
	virtual bool execute_step() = 0;
	virtual bool self_delete() const { return false; }

	uint64_t execute_loop(bool wff_call) {
		const t_time t0 = t_time::now();

		while (execute_step());

		const t_time t1 = t_time::now();
		const t_time dt = t1 - t0;

		if (!wff_call) {
			// do not set this from WFF, defeats the purpose
			assert(m_in_task_queue.load());
			m_in_task_queue.store(false);
		}

		if (self_delete())
			delete this;

		return (dt.to_ns());
	}

	bool is_finished() const { assert(m_remaining_tasks.load() >= 0); return (m_remaining_tasks.load(std::memory_order_relaxed) == 0); }
	bool is_in_queue() const { return (m_in_task_queue.load(std::memory_order_relaxed)); }
	bool is_in_pool() const { return (m_in_task_pool.load(std::memory_order_relaxed)); }

	int32_t remaining_tasks() const { return m_remaining_tasks; }
	int32_t wanted_thread() const { return m_wanted_thread; }

	bool wait_for(const t_time& rel_time) const {
		const t_time end_time = t_time::now() + rel_time;
		while (!is_finished() && (t_time::now() < end_time));
		return (is_finished());
	}

	uint32_t get_id() const { return m_id; }
	uint64_t get_dt(const t_time t) const { return (t.to_ns() - m_ts); }

	void update_id() { m_id = last_id.fetch_add(1); }
	void set_ts(const t_time t) { m_ts = t.to_ns(); }

	void reset_state(bool pooled) {
		m_remaining_tasks = 0;
		m_wanted_thread = 0;

		m_in_task_queue = true;
		m_in_task_pool = pooled;
	}

public:
	std::atomic<int32_t> m_remaining_tasks;
	// if 0 (default), task will be executed by an arbitrary thread
	std::atomic<int32_t> m_wanted_thread;

	std::atomic_bool m_in_task_queue;
	std::atomic_bool m_in_task_pool;

private:
	static std::atomic<uint32_t> last_id;

	uint32_t m_id;
	uint64_t m_ts; // timestamp (ns)
};



namespace thread_pool {
	template<class F, class... Args>
	static auto enqueue(F&& f, Args&&... args)
	-> std::shared_ptr<std::future<typename std::result_of<F(Args...)>::type>>;

	void push_task_group(t_base_task_group* task_group);
	void push_task_group(std::shared_ptr<t_base_task_group>&& task_group);
	void wait_for_finished(std::shared_ptr<t_base_task_group>&& task_group);

	// NOTE: do we want std::move here?
	template<typename T>
	inline void push_task_group(std::shared_ptr<T>& task_group) { push_task_group(std::move(std::static_pointer_cast<t_base_task_group>(task_group))); }
	template<typename T>
	inline void wait_for_finished(std::shared_ptr<T>& task_group) { wait_for_finished(std::move(std::static_pointer_cast<t_base_task_group>(task_group))); }

	void set_max_thread_count();
	void set_thread_count(uint32_t wanted_num_threads);
	void notify_threads(uint32_t force, uint32_t async);

	bool has_threads();

	uint32_t get_thread_num();
	uint32_t get_num_threads();
	uint32_t get_max_threads();

	static constexpr uint32_t MAX_THREADS = 16;
};



template<class F, class... Args>
class t_async_task: public t_base_task_group {
public:
	typedef typename std::result_of<F(Args...)>::type return_type;

	t_async_task(F f, Args... args): m_self_delete(true) {
		m_task = std::make_shared<std::packaged_task<return_type()>>(std::bind(f, std::forward<Args>(args)...));
		m_result = std::make_shared<std::future<return_type>>(m_task->get_future());

		m_remaining_tasks += 1;
	}

	bool is_async_task() const override { return true; }
	bool self_delete() const { return (m_self_delete.load()); }
	bool execute_step() override {
		// *never* called from wait_for_finished
		(*m_task)();
		m_remaining_tasks -= 1;
		return false;
	}

	// TODO: rethrow exceptions?
	std::shared_ptr<std::future<return_type>> get_future() { assert(m_result->valid()); return std::move(m_result); }

public:
	// if true, we are not managed by a shared_ptr
	std::atomic<bool> m_self_delete;

	std::shared_ptr<std::packaged_task<return_type()>> m_task;
	std::shared_ptr<std::future<return_type>> m_result;
};



template<class F, typename R = int, class... Args>
class t_ext_task_group: public t_base_task_group {
public:
	t_ext_task_group(const int32_t num = 0): m_cur_task(0) {
		m_results.reserve(num);
		m_tasks.reserve(num);
	}

	typedef R return_type;

	void enqueue(F f, Args... args) {
		auto task = std::make_shared<std::packaged_task<return_type()>>(std::bind(f, std::forward<Args>(args)...));

		m_results.emplace_back(task->get_future());
		m_tasks.emplace_back(task);
		m_remaining_tasks.fetch_add(1, std::memory_order_release);
	}


	bool execute_step() override {
		const int32_t pos = m_cur_task.fetch_add(1, std::memory_order_relaxed);

		if (pos < m_tasks.size()) {
			m_tasks[pos]();
			m_remaining_tasks.fetch_sub(1, std::memory_order_release);
			return true;
		}

		return false;
	}

	template<typename G>
	return_type GetResult(const G&& g) {
		return (std::accumulate(m_results.begin(), m_results.end(), 0, g));
	}

public:
	std::atomic<int32_t> m_cur_task;
	std::vector<std::function<void()>> m_tasks;
	std::vector<std::future<return_type>> m_results;
};




template<class F, typename ...Args>
class t_ext_task_group<F, void, Args...>: public t_base_task_group {
public:
	t_ext_task_group(const int32_t num = 0): m_cur_task(0) {
		m_tasks.reserve(num);
	}

	void enqueue(F f, Args... args) {
		m_tasks.emplace_back(std::bind(f, args...));
		m_remaining_tasks.fetch_add(1, std::memory_order_release);
	}

	bool execute_step() override {
		const int32_t pos = m_cur_task.fetch_add(1, std::memory_order_relaxed);

		if (pos < m_tasks.size()) {
			m_tasks[pos]();
			m_remaining_tasks.fetch_sub(1, std::memory_order_release);
			return true;
		}

		return false;
	}

public:
	std::atomic<int32_t> m_cur_task;
	std::vector<std::function<void()>> m_tasks;
};


template<class F>
class t_ext_task_group<F, void>: public t_base_task_group {
public:
	t_ext_task_group(const int32_t num = 0): m_cur_task(0) {
		m_tasks.reserve(num);
	}

	void enqueue(F f) {
		m_tasks.emplace_back(f);
		m_remaining_tasks.fetch_add(1, std::memory_order_release);
	}

	bool execute_step() override {
		const int32_t pos = m_cur_task.fetch_add(1, std::memory_order_relaxed);

		if (pos < m_tasks.size()) {
			m_tasks[pos]();
			m_remaining_tasks.fetch_sub(1, std::memory_order_release);
			return true;
		}

		return false;
	}

public:
	std::atomic<int32_t> m_cur_task;
	std::vector<std::function<void()>> m_tasks;
};




template<typename F, typename return_type = int, typename... Args>
class t_parallel_task_group: public t_ext_task_group<F, return_type, Args...> {
public:
	t_parallel_task_group(const int32_t num = 0): t_ext_task_group<F, return_type, Args...>(num) {
		m_unique_tasks.fill(nullptr);
	}

	void enqueue_unique(const int32_t thread_num, F& f, Args... args) {
		auto task = std::make_shared< std::packaged_task<return_type()> >(std::bind(std::forward<F>(f), std::forward<Args>(args)...));

		this->m_results.emplace_back(task->get_future());
		this->m_remaining_tasks += 1;

		// NOTE:
		//   here we want task <task> to be executed by thread <thread_num>
		//   but since any TG is pulled from the queue *once* by a (random)
		//   thread this does not actually happen
		m_unique_tasks[thread_num] = [=](){ (*task)(); };
	}

	bool execute_step() override {
		auto& func = m_unique_tasks[thread_pool::get_thread_num()];

		// does nothing when num=0 except return false (no change to m_remaining_tasks)
		if (func == nullptr)
			return (t_ext_task_group<F, return_type, Args...>::execute_step());

		func();
		func = nullptr;

		if (!this->is_finished()) {
			this->m_remaining_tasks -= 1;
			return true;
		}

		return false;
	}

public:
	std::array<std::function<void()>, thread_pool::MAX_THREADS> m_unique_tasks;
};


template<typename F>
class t_parallel_alt_task_group: public t_base_task_group {
public:
	t_parallel_alt_task_group(bool pooled): t_base_task_group(false, pooled) {}

	void enqueue(F& func) {
		m_func = func;
		m_remaining_tasks = thread_pool::get_num_threads();
		m_unique_tasks.fill(false);
	}

	bool execute_step() override {
		auto& ut = m_unique_tasks[thread_pool::get_thread_num()];

		if (!ut) {
			ut = true;

			m_func();
			m_remaining_tasks -= 1;

			return (!is_finished());
		}

		return false;
	}

public:
	std::array<bool, thread_pool::MAX_THREADS> m_unique_tasks;
	std::function<void()> m_func;
};




template<typename F, typename ...Args>
class t_arg_task_group: public t_ext_task_group<F, decltype(std::declval<F>()((std::declval<Args>())...)), Args...> {
public:
	typedef decltype(std::declval<F>()((std::declval<Args>())...)) R;

	t_arg_task_group(const int32_t num = 0): t_ext_task_group<F, R, Args...>(num) {}
};

template<typename F, typename ...Args>
class t_ext_parallel_task_group: public t_parallel_task_group<F, decltype(std::declval<F>()((std::declval<Args>())...)), Args...> {
public:
	typedef decltype(std::declval<F>()((std::declval<Args>())...)) R;

	t_ext_parallel_task_group(const int32_t num = 0): t_parallel_task_group<F, R, Args...>(num) {}
};



template<typename F>
class t_for_task_group: public t_base_task_group {
public:
	t_for_task_group(bool pooled): t_base_task_group(false, pooled), m_cur_task(0) {}

	void enqueue(const int32_t from, const int32_t to, const int32_t step, F& func) {
		assert(to >= from);
		m_remaining_tasks = (step == 1) ? (to - from): ((to - from + step - 1) / step);

		m_cur_task = {0};
		m_func = func;

		m_from = from;
		m_to   = to;
		m_step = step;
	}

	bool execute_step() override {
		const int32_t i = m_from + (m_step * m_cur_task.fetch_add(1, std::memory_order_relaxed));

		if (i < m_to) {
			m_func(i);
			m_remaining_tasks -= 1;
			return true;
		}

		return false;
	}

public:
	std::atomic<int32_t> m_cur_task;
	std::function<void(const int)> m_func;

	int32_t m_from;
	int32_t m_to;
	int32_t m_step;
};








template <template<typename> class TG, typename F>
struct t_task_pool {
	typedef TG<F> func_task_group;
	typedef std::shared_ptr<func_task_group> func_task_group_ptr;

	// more than 256 nested for_mt's or parallel's should be uncommon
	std::array<func_task_group_ptr, 256> m_tg_pool;
	std::atomic<int32_t> m_pool_idx = {0};

	t_task_pool() {
		for (int32_t i = 0; i < m_tg_pool.size(); ++i) {
			m_tg_pool[i] = func_task_group_ptr(new func_task_group(true));
		}
	}

	func_task_group_ptr get_task_group() {
		auto tg = m_tg_pool[m_pool_idx.fetch_add(1) % m_tg_pool.size()];

		assert(tg->is_finished());
		assert(tg->is_in_pool());

		tg->reset_state(true);
		return tg;
	}
};




template <typename F>
static inline void for_mt(int32_t start, int32_t end, int32_t step, F&& f) {
	if (!thread_pool::has_threads() || ((end - start) < step)) {
		for (int32_t i = start; i < end; i += step) {
			f(i);
		}
		return;
	}

	// static, so task_group's are recycled
	static t_task_pool<t_for_task_group, F> pool;
	auto task_group = pool.get_task_group();

	task_group->enqueue(start, end, step, f);
	task_group->update_id();
	thread_pool::push_task_group(task_group);
	thread_pool::wait_for_finished(task_group); // make calling thread also run execute_loop
}

template <typename F>
static inline void for_mt(int32_t start, int32_t end, F&& f) {
	for_mt(start, end, 1, f);
}



template <typename F>
static inline void parallel(F&& f) {
	if (!thread_pool::has_threads())
		return f();

	// static, so task_group's are recycled
	static t_task_pool<t_parallel_alt_task_group, F> pool;
	auto task_group = pool.get_task_group();

	task_group->enqueue(f);
	task_group->update_id();
	thread_pool::push_task_group(task_group);
	thread_pool::wait_for_finished(task_group);
}

template<class F, class G>
static inline auto parallel_reduce(F&& f, G&& g) -> typename std::result_of<F()>::type {
	if (!thread_pool::has_threads())
		return (f());

	typedef  typename std::result_of<F()>::type  ret_type;
	// typedef  typename std::shared_ptr< t_async_task<F> >  task_type;
	typedef           std::shared_ptr< std::future<ret_type> >  fold_type;

	// std::vector<task_type> tasks(thread_pool::get_num_threads());
	std::vector<t_async_task<F>*> tasks(thread_pool::get_num_threads(), nullptr);
	std::vector<fold_type> results(thread_pool::get_num_threads());

	// NOTE:
	//   results become available in async_task::execute_step, and can allow
	//   accumulate to return (followed by tasks going out of scope) before
	//   execute_step's themselves have returned --> premature task deletion
	//   if shared_ptr were used (all tasks *must* have exited execute_loop)
	//
	#if 0
	tasks[0] = std::move(std::make_shared<t_async_task<F>>(std::forward<F>(f)));
	#endif
	tasks[0] = new t_async_task<F>(std::forward<F>(f));
	results[0] = std::move(tasks[0]->get_future());

	// first job usually wants to run on the main thread
	tasks[0]->execute_loop(false);

	// need to push N individual tasks; see NOTE in t_parallel_task_group
	for (size_t i = 1; i < results.size(); ++i) {
		#if 0
		tasks[i] = std::move(std::make_shared<t_async_task<F>>(std::forward<F>(f)));
		#endif
		tasks[i] = new t_async_task<F>(std::forward<F>(f));
		results[i] = std::move(tasks[i]->get_future());

		#if 0
		tasks[i]->m_self_delete.store(false);
		#endif
		tasks[i]->m_wanted_thread.store(i);

		thread_pool::push_task_group(tasks[i]);
	}

	return (std::accumulate(results.begin(), results.end(), 0, g));
}




namespace thread_pool {
	template<class F, class... Args>
	static inline auto enqueue(F&& f, Args&&... args)
	-> std::shared_ptr<std::future<typename std::result_of<F(Args...)>::type>> {
		typedef typename std::result_of<F(Args...)>::type return_type;

		if (!thread_pool::has_threads()) {
			auto task = std::make_shared< std::packaged_task<return_type()> >(std::bind(f, args ...));
			auto fut = std::make_shared<std::future<return_type>>(task->get_future());

			(*task)();
			return fut;
		}

		// can not use shared_ptr here, make async tasks delete themselves instead
		// auto task = std::make_shared<t_async_task<F, Args...>>(std::forward<F>(f), std::forward<Args>(args)...);
		auto task = new t_async_task<F, Args...>(std::forward<F>(f), std::forward<Args>(args)...);
		auto fut = task->get_future();

		// minor hack: assume async_task's are never allowed to block the main thread,
		// so do not put any in the global queue which is serviced by it during calls
		// to wait_for_finished
		task->m_wanted_thread.store(1 + task->get_id() % (thread_pool::get_num_threads() - 1));

		thread_pool::push_task_group(task);
		return fut;
	}
};

#endif
#endif

