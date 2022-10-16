#pragma once

#include <stdint.h>

namespace engine {

struct DateUtils {

	// from https://github.com/TimoKersten/db-engine-paradigms/blob/ae3286b279ad26ab294224d630d650bc2f2f3519/src/common/runtime/Types.cpp
	static inline unsigned
	mergeJulianDay(unsigned year, unsigned month, unsigned day)
	{
		unsigned a = (14 - month) / 12;
		unsigned y = year + 4800 - a;
		unsigned m = month + (12*a) - 3;

		return day + ((153*m+2)/5) + (365*y) + (y/4) - (y/100) + (y/400) - 32045;
	}

	static inline void
	splitJulianDay(unsigned jd, unsigned& year, unsigned& month, unsigned& day)
		// Algorithm from the Calendar FAQ
	{
		unsigned a = jd + 32044;
		unsigned b = (4*a+3)/146097;
		unsigned c = a-((146097*b)/4);
		unsigned d = (4*c+3)/1461;
		unsigned e = c-((1461*d)/4);
		unsigned m = (5*e+2)/153;

		day = e - ((153*m+2)/5) + 1;
		month = m + 3 - (12*(m/10));
		year = (100*b) + d - 4800 + (m/10);
	}
};

} /* engine */