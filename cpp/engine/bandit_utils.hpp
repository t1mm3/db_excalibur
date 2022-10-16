#pragma once

#include <vector>
#include <cstddef>

#include <math.h>
#include <limits>

struct Bandit {
protected:
	double round = 0.0;

	const double c;
	const size_t num_arms;

	static constexpr double kDefaultConstant = 2.0/3.0;

	struct ArmInfo {
		size_t count = 0;
		double rewards = 0.0;

		void reset() {
			count = 0;
			rewards = 0.0;
		}
	};

	ArmInfo* arms;

	Bandit(size_t num_arms, double constant)
	: c(constant), num_arms(num_arms)
	{

	}

	template<size_t NUM_ARMS>
	static size_t choose_arm(const ArmInfo* arms, size_t _num,
		double c, double round)
	{
		size_t num = NUM_ARMS ? NUM_ARMS : _num;
		auto compute_ucb = [&] (auto& arm) {
			if (!arm.count) {
				return std::numeric_limits<double>::infinity();
			}

			double count = arm.count;
			double mean = arm.rewards / count;

			return mean + sqrt(c * log10(round) / count);
		};

		double max_val = compute_ucb(arms[0]);
		size_t max_arm = 0;
		for (size_t i=1; i<num; i++) {
			double val = compute_ucb(arms[i]);
			if (val > max_val) {
				max_val = val;
				max_arm = i;
			}
		}
		return max_arm;
	}

public:

	void record(size_t arm, double reward, size_t runs) {
		round += runs;

		auto& a = arms[arm];
		a.count += runs;
		a.rewards += reward;
	}

	// size_t choose_arm() const { return choose_arm(arms, num_arms, c, round); }

	virtual ~Bandit() = default;
};

struct BanditDynArms : Bandit {
	BanditDynArms(size_t num_arms, double constant = kDefaultConstant)
	 : Bandit(num_arms, constant)
	{
		alloc_arms.resize(num_arms);
		arms = &alloc_arms[0];
	}

	size_t choose_arm() const {
		return Bandit::choose_arm<0>(&alloc_arms[0], num_arms, c, round);
	}

private:
	std::vector<ArmInfo> alloc_arms;
};

template<size_t NUM_ARMS>
struct BanditStaticArms : Bandit {
	BanditStaticArms(double constant = kDefaultConstant)
	: Bandit(NUM_ARMS, constant) {
		for (size_t a=0; a<NUM_ARMS; a++) {
			alloc_arms[a].reset();
		}

		arms = &alloc_arms[0];
	}

	size_t choose_arm() const {
		return Bandit::choose_arm<NUM_ARMS>(&alloc_arms[0], NUM_ARMS, c, round);
	}

private:
	ArmInfo alloc_arms[NUM_ARMS];
};