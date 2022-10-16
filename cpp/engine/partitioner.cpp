#include "partitioner.hpp"

#include <string.h>

using namespace engine;

#define EXPAND_PREDEFINED_PARTITIONS(F, ARGS) \
F(1, ARGS)\
F(2, ARGS)\
F(3, ARGS)\
F(4, ARGS)\
F(5, ARGS)\
F(6, ARGS)\
F(7, ARGS)\
F(8, ARGS)\
F(9, ARGS)\
F(10, ARGS)\
F(11, ARGS)\
F(12, ARGS)\
F(13, ARGS)\
F(14, ARGS)\
F(15, ARGS)\
F(16, ARGS)\
F(17, ARGS)\
F(18, ARGS)\
F(19, ARGS)\
F(20, ARGS)\
F(21, ARGS)\
F(22, ARGS)\
F(23, ARGS)\
F(24, ARGS)\
F(25, ARGS)\
F(26, ARGS)\
F(27, ARGS)\
F(28, ARGS)\
F(29, ARGS)\
F(30, ARGS)\
F(31, ARGS)\
F(32, ARGS)\
F(33, ARGS)\
F(34, ARGS)\
F(35, ARGS)\
F(36, ARGS)\
F(37, ARGS)\
F(38, ARGS)\
F(39, ARGS)\
F(40, ARGS)\
F(41, ARGS)\
F(42, ARGS)\
F(43, ARGS)\
F(44, ARGS)\
F(45, ARGS)\
F(46, ARGS)\
F(47, ARGS)\
F(48, ARGS)\
F(49, ARGS)\
F(50, ARGS)\
F(51, ARGS)\
F(52, ARGS)\
F(53, ARGS)\
F(54, ARGS)\
F(55, ARGS)\
F(56, ARGS)\
F(57, ARGS)\
F(58, ARGS)\
F(59, ARGS)\
F(60, ARGS)\
F(61, ARGS)\
F(62, ARGS)\
F(63, ARGS)\
F(64, ARGS)\
F(65, ARGS)\
F(66, ARGS)\
F(67, ARGS)\
F(68, ARGS)\
F(69, ARGS)\
F(70, ARGS)\
F(71, ARGS)\
F(72, ARGS)\
F(73, ARGS)\
F(74, ARGS)\
F(75, ARGS)\
F(76, ARGS)\
F(77, ARGS)\
F(78, ARGS)\
F(79, ARGS)\
F(80, ARGS)\
F(81, ARGS)\
F(82, ARGS)\
F(83, ARGS)\
F(84, ARGS)\
F(85, ARGS)\
F(86, ARGS)\
F(87, ARGS)\
F(88, ARGS)\
F(89, ARGS)\
F(90, ARGS)\
F(91, ARGS)\
F(92, ARGS)\
F(93, ARGS)\
F(94, ARGS)\
F(95, ARGS)\
F(96, ARGS)\
F(97, ARGS)\
F(98, ARGS)\
F(99, ARGS)\
F(100, ARGS)\
F(101, ARGS)\
F(102, ARGS)\
F(103, ARGS)\
F(104, ARGS)\
F(105, ARGS)\
F(106, ARGS)\
F(107, ARGS)\
F(108, ARGS)\
F(109, ARGS)\
F(110, ARGS)\
F(111, ARGS)\
F(112, ARGS)\
F(113, ARGS)\
F(114, ARGS)\
F(115, ARGS)\
F(116, ARGS)\
F(117, ARGS)\
F(118, ARGS)\
F(119, ARGS)\
F(120, ARGS)\
F(121, ARGS)\
F(122, ARGS)\
F(123, ARGS)\
F(124, ARGS)\
F(125, ARGS)\
F(126, ARGS)\
F(127, ARGS)\
F(128, ARGS)\
F(129, ARGS)\
F(130, ARGS)\
F(131, ARGS)\
F(132, ARGS)\
F(133, ARGS)\
F(134, ARGS)\
F(135, ARGS)\
F(136, ARGS)\
F(137, ARGS)\
F(138, ARGS)\
F(139, ARGS)\
F(140, ARGS)\
F(141, ARGS)\
F(142, ARGS)\
F(143, ARGS)\
F(144, ARGS)\
F(145, ARGS)\
F(146, ARGS)\
F(147, ARGS)\
F(148, ARGS)\
F(149, ARGS)\
F(150, ARGS)\
F(151, ARGS)\
F(152, ARGS)\
F(153, ARGS)\
F(154, ARGS)\
F(155, ARGS)\
F(156, ARGS)\
F(157, ARGS)\
F(158, ARGS)\
F(159, ARGS)\
F(160, ARGS)\
F(161, ARGS)\
F(162, ARGS)\
F(163, ARGS)\
F(164, ARGS)\
F(165, ARGS)\
F(166, ARGS)\
F(167, ARGS)\
F(168, ARGS)\
F(169, ARGS)\
F(170, ARGS)\
F(171, ARGS)\
F(172, ARGS)\
F(173, ARGS)\
F(174, ARGS)\
F(175, ARGS)\
F(176, ARGS)\
F(177, ARGS)\
F(178, ARGS)\
F(179, ARGS)\
F(180, ARGS)\
F(181, ARGS)\
F(182, ARGS)\
F(183, ARGS)\
F(184, ARGS)\
F(185, ARGS)\
F(186, ARGS)\
F(187, ARGS)\
F(188, ARGS)\
F(189, ARGS)\
F(190, ARGS)\
F(191, ARGS)\
F(192, ARGS)\
F(193, ARGS)\
F(194, ARGS)\
F(195, ARGS)\
F(196, ARGS)\
F(197, ARGS)\
F(198, ARGS)\
F(199, ARGS)\
F(200, ARGS)

#define EXPAND_PREDEFINED_WIDTHS(F, ARGS) \
F(1, ARGS)\
F(2, ARGS)\
F(4, ARGS)\
F(8, ARGS)\
F(12, ARGS)\
F(16, ARGS)\
F(20, ARGS)\
F(24, ARGS)\
F(28, ARGS)\
F(32, ARGS)\
F(36, ARGS)\
F(40, ARGS)\
F(44, ARGS)\
F(48, ARGS)\
F(52, ARGS)\
F(56, ARGS)\
F(60, ARGS)\
F(64, ARGS)\
F(68, ARGS)\
F(72, ARGS)\
F(76, ARGS)\
F(80, ARGS)\
F(84, ARGS)\
F(88, ARGS)\
F(92, ARGS)\
F(96, ARGS)\
F(100, ARGS)\
F(104, ARGS)\
F(108, ARGS)\
F(112, ARGS)\
F(116, ARGS)\
F(120, ARGS)\
F(124, ARGS)

template<size_t PARTITIONS, size_t WIDTH>
static NOINLINE void
_runtime_partition(size_t* RESTRICT dest_counts,
char** RESTRICT dest_data, size_t _num_parts, char* RESTRICT data,
size_t _width, uint64_t* hashes, size_t num, size_t _offset)
{
	const size_t partitions = PARTITIONS ? PARTITIONS : _num_parts;
	const size_t width = WIDTH ? WIDTH : _width;
	ASSERT(_num_parts == partitions);
	ASSERT(_width == width);

	if (_offset) {
		size_t offset = _offset;
		for (size_t i=0; i<num; i++) {
			const uint64_t hash = hashes[i];
			const uint64_t p = hash % partitions;
			char* src = data + (i + offset) * width;
			char* dest = dest_data[p];
			memmove(dest, src, width);
			dest_data[p] += width;
			dest_counts[p] ++;
		}
	} else {
		size_t offset = 0;
		for (size_t i=0; i<num; i++) {
			const uint64_t hash = hashes[i];
			const uint64_t p = hash % partitions;
			char* src = data + (i + offset) * width;
			char* dest = dest_data[p];
			memmove(dest, src, width);
			dest_data[p] += width;
			dest_counts[p] ++;
		}
	}
}

template<size_t PARTITIONS>
static NOINLINE void
_compute_partition(uint64_t* RESTRICT partition_id, uint64_t* RESTRICT dest_offset,
	size_t* RESTRICT dest_counts, size_t _num_parts,
	uint64_t* hashes, size_t num)
{
	const size_t partitions = PARTITIONS ? PARTITIONS : _num_parts;
	ASSERT(_num_parts == partitions);

	for (size_t i=0; i<num; i++) {
		const uint64_t hash = hashes[i];
		const uint64_t p = hash % partitions;
		partition_id[i] = p;
		dest_offset[i] = dest_counts[p];
		dest_counts[p] ++;
	}
}

template<size_t WIDTH>
static NOINLINE void
_get_dest_ptr(char** RESTRICT dest_ptr, char** RESTRICT dest_data,
	uint64_t* RESTRICT partition_id, uint64_t* RESTRICT dest_offset,
	size_t _width, size_t num)
{
	const size_t width = WIDTH ? WIDTH : _width;
	ASSERT(_width == width);

	for (size_t i=0; i<num; i++) {
		auto p = partition_id[i];
		dest_ptr[i] = dest_data[p] + dest_offset[i]*width;
	}
}

template<size_t WIDTH>
static NOINLINE void
_scatter(char** RESTRICT dest_ptrs, char* RESTRICT source, size_t _width,
	size_t num)
{
	const size_t width = WIDTH ? WIDTH : _width;
	ASSERT(_width == width);

	for (size_t i=0; i<num; i++) {
		char* src = source + i * width;
		char* dest = dest_ptrs[i];
		memmove(dest, src, width);
	}
}



Partitioner::runtime_partition_call_t
Partitioner::get_runtime_partition(size_t num_parts, size_t width)
{
	switch (num_parts) {
#define CASE_WIDTH(WIDTH, PARTITIONS) \
		case WIDTH: return &_runtime_partition<PARTITIONS, WIDTH>;

#define CASE_PARTITION(PARTITIONS, ARGS) \
		case PARTITIONS: \
			switch (width) { \
				EXPAND_PREDEFINED_WIDTHS(CASE_WIDTH, PARTITIONS) \
				default: break;	 \
			} \
			return (runtime_partition_call_t)&_runtime_partition<PARTITIONS, 0>;
EXPAND_PREDEFINED_PARTITIONS(CASE_PARTITION, _)

#undef CASE_PARTITION
#undef CASE_WIDTH

	case 0:
		ASSERT(false);
		return nullptr;
	default:
		break;
	}

	return (runtime_partition_call_t)&_runtime_partition<0, 0>;
}


Partitioner::compute_partition_call_t
Partitioner::get_compute_partition(size_t num_parts)
{
	switch (num_parts) {
#define CASE_PARTITION(PARTITIONS, ARGS) \
		case PARTITIONS: \
			return (compute_partition_call_t)&_compute_partition<PARTITIONS>;
EXPAND_PREDEFINED_PARTITIONS(CASE_PARTITION, _)

#undef CASE_PARTITION

	case 0:
		ASSERT(false);
		return nullptr;
	default:
		break;
	}

	return (compute_partition_call_t)&_compute_partition<0>;
}

Partitioner::compute_dest_pointers_call_t
Partitioner::get_compute_dest_pointers(size_t width)
{
	switch (width) {
#define CASE_WIDTH(WIDTH, ARGS) \
		case WIDTH: \
			return (compute_dest_pointers_call_t)&_get_dest_ptr<WIDTH>;
EXPAND_PREDEFINED_PARTITIONS(CASE_WIDTH, _)

#undef CASE_WIDTH

	case 0:
		ASSERT(false);
		return nullptr;
	default:
		break;
	}

	return (compute_dest_pointers_call_t)&_get_dest_ptr<0>;
}


Partitioner::scatter_call_t
Partitioner::get_scatter(size_t width)
{
	switch (width) {
#define CASE_WIDTH(WIDTH, ARGS) \
		case WIDTH: \
			return (scatter_call_t)&_scatter<WIDTH>;
EXPAND_PREDEFINED_PARTITIONS(CASE_WIDTH, _)

#undef CASE_WIDTH

	case 0:
		ASSERT(false);
		return nullptr;
	default:
		break;
	}

	return (scatter_call_t)&_scatter<0>;
}