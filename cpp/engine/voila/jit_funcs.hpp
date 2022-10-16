/** This file is included multiple times from jit.cpp to generate
  * prototypes and implementation of the following JIT primitives
  */

// leave NOP here, i.e. 0 bytes are NOPs
VM_PRIM_BEGIN(nop)
#ifdef VM_PRIM_IMPL
#endif
VM_PRIM_END(nop)


VM_PRIM_BEGIN(end)
#ifdef VM_PRIM_IMPL
	// reset IP
	VM_PRIM_RETURN(end, CodeBlock::kInterpretEnd, 0);
#endif
VM_PRIM_END_EX(end, false)



VM_PRIM_BEGIN(goto_cond)
#ifdef VM_PRIM_IMPL
	auto old_ip = __ip;
	auto& item = *context->item;
	auto& data = item.data.go;

	const char* goto_name = data.value ? "goto_true" : "goto_false";

	VM_PRIM_ASSERT(data.predicate->tag == XValue::Tag::kPredicate);

	VM_PRIM_DEBUG("CALL [%llu] %s: pred=%p num=%lld",
		old_ip, goto_name, data.predicate, data.predicate->num);

	engine::util_kernels::debug_assert_valid_selection_vector<selvector_t>(
		data.predicate->get_first_as<selvector_t>(), data.predicate->num);

#if 0
	if ((data.predicate->num == 0) == data.value)  {
		VM_PRIM_DEBUG("CALL [%llu] %s: no jump", old_ip, goto_name);
		__ip++;
	} else {
		__ip = data.offset;
		VM_PRIM_DEBUG("CALL [%llu] %s: jump %llu",
			old_ip, goto_name, data.offset);
		VM_PRIM_ASSERT(__ip < context->num_codes);
	}
#else
	__ip = (data.predicate->num == 0) == data.value ?
		__ip + 1 : data.offset;
#endif
#endif
VM_PRIM_END_EX(goto_cond, false)



VM_PRIM_BEGIN(goto_uncond)
#ifdef VM_PRIM_IMPL
	auto old_ip = __ip;
	VM_PRIM_DEBUG("CALL [%llu] goto_uncond", old_ip);
	auto& item = *context->item;
	auto& data = item.data.go;

	__ip = data.offset;
	VM_PRIM_DEBUG("CALL [%llu] goto_uncond: jump %llu",
		old_ip, __ip);
	VM_PRIM_ASSERT(__ip < context->num_codes);
#endif
VM_PRIM_END_EX(goto_uncond, false)




VM_PRIM_BEGIN(copy)
#ifdef VM_PRIM_IMPL
	VM_PRIM_PREFETCH_NEXT();

	auto& item = *context->item;
	auto& data = item.data.copy;
	auto& result = item.result;
	auto& source = data.source;
	auto& predicate = data.predicate;

	VM_PRIM_DEBUG("CALL [%llu] copy from %p to %p with predicate %p",
		__ip, source, result, predicate);

	if (data.predicate) {
		engine::util_kernels::debug_assert_valid_selection_vector<selvector_t>(
			data.predicate->get_first_as<selvector_t>(), data.predicate->num);
	}

	XValue::copy<false>(*result, *source);

	VM_PRIM_DEBUG("source: done=%d, num=%lld", source->done, source->num);
	VM_PRIM_DEBUG("result: done=%d, num=%lld", result->done, result->num);
#endif
VM_PRIM_END(copy)



VM_PRIM_BEGIN(end_of_flow)
#ifdef VM_PRIM_IMPL
	VM_PRIM_DEBUG("CALL [%llu] end_of_flow", __ip);

	auto& predicate = context->block->op_output->predicate;
	if (predicate) {
		predicate->done = true;
		predicate->num = 0;
	}

	auto& output = context->block->op_output->col_data;
	for (auto& out : output) {
		out->done = true;
		out->num = 0;
	}

	// reset IP
	VM_PRIM_RETURN(end_of_flow, CodeBlock::kInterpretDone, 0);
#endif
VM_PRIM_END_EX(end_of_flow, false)



VM_PRIM_BEGIN(comment)
#ifdef VM_PRIM_IMPL
	VM_PRIM_PREFETCH_NEXT();

	auto& item = *context->item;
	auto& data = item.data.comment;

	if (!data.predicate || data.predicate->num) {
		LOG_MESSAGE(data.log_level, "CALL [%llu] comment %s", __ip,
			(const char*)data.message);
	}
#endif
VM_PRIM_END(comment)



VM_PRIM_BEGIN(tuple_emit)
#ifdef VM_PRIM_IMPL
	VM_PRIM_DEBUG("CALL [%llu] tuple_emit", __ip);
	VM_PRIM_ASSERT(context->block->op_output);
	auto& data = context->item->data.tuple_emit;
	auto& extra = data.args;
	VM_PRIM_ASSERT(extra);

	const auto& args = extra->args;
	auto& output = context->block->op_output->col_data;
	if (UNLIKELY(args.size() != output.size())) {
		output.resize(args.size());
	}

	size_t i=0;
	for (auto& arg : args) {
		output[i] = arg;
		LOG_TRACE("tuple_emit: col_data %p", output[i]);
		i++;
	}

	context->block->op_output->predicate = data.predicate;

	if (data.predicate) {
		engine::util_kernels::debug_assert_valid_selection_vector<selvector_t>(
			data.predicate->get_first_as<selvector_t>(), data.predicate->num);

		VM_PRIM_DEBUG("tuple_emit: pred %d, %p", data.predicate->num, data.predicate);
	}

	// since we jump out, DISPATCH is not going to increment IP
	VM_PRIM_RETURN(tuple_emit, CodeBlock::kInterpretYield, __ip+1);
#endif
VM_PRIM_END(tuple_emit)



VM_PRIM_BEGIN(emit)
#ifdef VM_PRIM_IMPL
	VM_PRIM_DEBUG("CALL [%llu] emit", __ip);
	VM_PRIM_ASSERT(context->block->op_output);

	// since we jump out, DISPATCH is not going to increment IP
	VM_PRIM_RETURN(emit, CodeBlock::kInterpretYield, __ip+1);
#endif
VM_PRIM_END(emit)



VM_PRIM_BEGIN(selnum)
#ifdef VM_PRIM_IMPL
	VM_PRIM_DEBUG("CALL [%llu] selnum", __ip);
	auto& item = *context->item;

	auto scan_pos_xval = context->item->data.selnum.pos;
	VM_PRIM_ASSERT(scan_pos_xval->tag == XValue::Tag::kPosition);

	auto& result = item.result;
	// VM_PRIM_ASSERT(result->match_flags(XValue::kConstVector));
	VM_PRIM_ASSERT(!result->match_flags(XValue::kConstStruct));
	
	result->done = scan_pos_xval->done;
	result->num = scan_pos_xval->num;

	engine::util_kernels::debug_assert_valid_selection_vector<selvector_t>(
		result->get_first_as<selvector_t>(), result->num);

	LOG_TRACE("TODO: SelNum: Avoid selection vector creation #3");
#endif
VM_PRIM_END(selnum)



VM_PRIM_BEGIN(scan_pos)
#ifdef VM_PRIM_IMPL
	VM_PRIM_PREFETCH_NEXT();

	auto ip = __ip;
	VM_PRIM_DEBUG("CALL [%llu] scan_pos", ip);
	auto& item = *context->item;
	auto& data = item.data.scan_pos;
	auto read_pos = data.read_pos;

	VM_PRIM_ASSERT(read_pos);
	if (UNLIKELY(!read_pos->num_calls
			|| read_pos->num_consumed >= read_pos->num_tuples)) {
		auto morsel_size = context->block->morsel_size;
		VM_PRIM_DEBUG("CALL [%llu] scan_pos: out of tuples: "
			"num_consumed %llu, num_tuples %llu morsel_size %llu",
			ip, read_pos->num_consumed, read_pos->num_tuples,
			morsel_size);
		ASSERT(morsel_size > 0);

		bool fetch_next = true;

		if (LIKELY(read_pos->num_calls > 0)) {
			context->block->stream.scan_feedback(*read_pos, fetch_next);
		}

		if (LIKELY(fetch_next)) {
			// Read base table
			read_pos->done = false;
			read_pos->next(morsel_size);
			read_pos->num_consumed = 0;

			context->block->stream.scan_feedback_with_tuples(*read_pos);
		} else {
			read_pos->done = true;
		}

		read_pos->num_calls++;
	}

	auto& result = item.result;
	VM_PRIM_ASSERT(!result->match_flags(XValue::kConstStruct));
	result->first = read_pos->column_data;
	result->done = read_pos->done;
	result->offset = read_pos->num_consumed;
	result->side_pointer = read_pos;

	if (UNLIKELY(result->done)) {
		result->num = 0;
		result->offset = 0;
	} else {
		VM_PRIM_ASSERT(read_pos->num_consumed <= read_pos->num_tuples);
		result->num = std::min(data.vector_size,
			read_pos->num_tuples - read_pos->num_consumed ); 

		read_pos->num_consumed += result->num;
	}
	read_pos->num_flow = result->num;

	VM_PRIM_DEBUG("CALL [%llu] scan_pos: num %llu, num_consumed %llu, "
		"num_tuples %llu, vsize %llu, done %d",
		ip, result->num, read_pos->num_consumed, read_pos->num_tuples,
		data.vector_size, (int)result->done);

	if (result->done) {
		VM_PRIM_ASSERT(!result->num)
	} else {
		VM_PRIM_ASSERT(result->num)
	}
#endif
VM_PRIM_END(scan_pos)



VM_PRIM_BEGIN(scan_col)
#ifdef VM_PRIM_IMPL
	VM_PRIM_PREFETCH_NEXT();

	VM_PRIM_DEBUG("CALL [%llu] scan_col", __ip);
	auto& item = *context->item;
	auto& data = item.data.scan_col;
	auto read_pos =
		static_cast<voila::ReadPosition*>(data.read_pos->side_pointer);
	VM_PRIM_ASSERT(read_pos);

	auto col = data.col_index;
	VM_PRIM_ASSERT(col >= 0 && col < read_pos->num_columns);

	auto& result = item.result;

	VM_PRIM_ASSERT(read_pos->num_consumed >= read_pos->num_flow);
	VM_PRIM_ASSERT(result->get_width());
	VM_PRIM_ASSERT(!result->match_flags(XValue::kConstVector | XValue::kConstStruct));

	int64_t row_offset = read_pos->num_consumed - read_pos->num_flow;
	VM_PRIM_ASSERT(row_offset == (int64_t)data.read_pos->offset);

	auto byte_offset = row_offset * result->get_width();

	result->first = read_pos->column_data[col] + byte_offset;
	result->num = read_pos->num_flow;
	result->done = read_pos->done;
#endif
VM_PRIM_END(scan_col)



VM_PRIM_BEGIN(scan_multiple_cols)
#ifdef VM_PRIM_IMPL
	VM_PRIM_PREFETCH_NEXT();

	VM_PRIM_DEBUG("CALL [%llu] scan_multiple_cols", __ip);
	auto& item = *context->item;
	auto& data = item.data.scan_multiple_cols;
	auto read_pos =
		static_cast<voila::ReadPosition*>(data.read_pos->side_pointer);
	VM_PRIM_ASSERT(read_pos);

	VM_PRIM_ASSERT(read_pos->num_consumed >= read_pos->num_flow);
	int64_t row_offset = read_pos->num_consumed - read_pos->num_flow;
	VM_PRIM_ASSERT(row_offset == (int64_t)data.read_pos->offset);

	auto& result = item.result;
	auto state = (MultiColScanState*)data.state;
	ASSERT(state && state->size());
	ASSERT(!result);

	for (auto& pair : state->col_map) {
		auto result = pair.first;
		auto col = pair.second;

		VM_PRIM_ASSERT(col >= 0 && col < read_pos->num_columns);
		VM_PRIM_ASSERT(result->get_width());
		VM_PRIM_ASSERT(!result->match_flags(XValue::kConstVector | XValue::kConstStruct));

		auto byte_offset = row_offset * result->get_width();

		result->first = read_pos->column_data[col] + byte_offset;
		result->num = read_pos->num_flow;
		result->done = read_pos->done;
	}

#endif
VM_PRIM_END(scan_multiple_cols)



VM_PRIM_BEGIN(write_pos)
#ifdef VM_PRIM_IMPL
	VM_PRIM_PREFETCH_NEXT();

	VM_PRIM_DEBUG("CALL [%llu] write_pos", __ip);
	auto& item = *context->item;
	auto& data = item.data.write_pos;

	VM_PRIM_ASSERT(data.predicate);

	auto& result = item.result;
	size_t num = data.predicate->num;
	VM_PRIM_ASSERT(!result->match_flags(XValue::kConstVector | XValue::kConstStruct));
	if (UNLIKELY(!num || data.predicate->done)) {
		result->first = nullptr;
		result->done = data.predicate->done;
		result->num = num;
		result->offset = 0;
	} else {
		auto table = data.table;
		VM_PRIM_ASSERT(table);
		const auto thread_id = context->block->next_context->thread_id;
		const auto numa_node = context->block->next_context->numa_node;
		VM_PRIM_ASSERT(thread_id >= 0);

		size_t offset = 0;
		char* block_data = nullptr;
		table->get_write_pos(offset, block_data, num, thread_id, numa_node);
		VM_PRIM_ASSERT(block_data);

		VM_PRIM_DEBUG("write_pos: block_data %p, offset %llu, num %llu", block_data, offset, num);

		result->first = block_data + offset;
		result->done = false;
		result->num = num;
		result->offset = 0;
	}
#endif
VM_PRIM_END(write_pos)



VM_PRIM_BEGIN(bucket_insert)
#ifdef VM_PRIM_IMPL
	VM_PRIM_PREFETCH_NEXT();

	VM_PRIM_DEBUG("CALL [%llu] bucket_insert", __ip);
	auto& item = *context->item;
	auto& result = item.result;
	VM_PRIM_ASSERT(!result->match_flags(XValue::kConstVector | XValue::kConstStruct));
	auto& data = item.data.bucket_insert;
	VM_PRIM_ASSERT(data.predicate);
	VM_PRIM_ASSERT(data.indices);

	size_t num = data.predicate->get_num();
	if (num) {
		num = vec_bucket_insert(
			result->get_first_as<uint64_t>(),
			data.table,
			data.indices->get_first_as<uint64_t>(),
			data.predicate->get_first_as<selvector_t>(),
			num,
			context->block->next_context->numa_node);
		VM_PRIM_DEBUG("inserted %llu buckets", num);
	} else {
		result->num = 0;
		// point to zeros
	}
#endif
VM_PRIM_END(bucket_insert)



VM_PRIM_BEGIN(selunion)
#ifdef VM_PRIM_IMPL
	VM_PRIM_PREFETCH_NEXT();

	VM_PRIM_DEBUG("CALL [%llu] selunion", __ip);
	auto& item = *context->item;
	auto& result = item.result;
	auto& data = item.data.selunion;
	auto pred = data.predicate;
	auto other = data.other;
	auto tmp = data.tmp;
	VM_PRIM_ASSERT(pred && other && tmp);
	VM_PRIM_ASSERT(!result->match_flags(XValue::kConstVector | XValue::kConstStruct));

	result->num = selunion<selvector_t, true>(
		result->get_first_as<selvector_t>(), result->get_capacity(),
		pred->get_first_as<selvector_t>(), pred->get_num(),
		other->get_first_as<selvector_t>(), other->get_num(),
		tmp->get_first_as<selvector_t>());
	result->done = data.predicate->done && data.other->done;

	LOG_TRACE("selunion %p, first=%p, num=%lld, done=%d",
		result, result->first, result->num, result->done);
#endif
VM_PRIM_END(selunion)



// fragment possibly in compilation
VM_PRIM_BEGIN(compiling_fragment)
#ifdef VM_PRIM_IMPL
	auto& item = *context->item;
	auto& data = item.data.compiling_fragment;
	VM_PRIM_DEBUG("CALL [%llu] compiling_fragment with call %p",
		__ip, data.call);
	if (UNLIKELY(!data.call)) {
		VM_PRIM_DEBUG("wait until fragment is compiled");
		auto jit_arg = (JitPrimArg*)data.prim_arg;
		auto& future = jit_arg->block_future;
		ASSERT(future);

		// wait until fragment is compiled and cache result
		auto fragment_ptr = compiling_fragment_get(*future);
		data.call = (CodeBlock::call_t)fragment_ptr->call;
		VM_PRIM_DEBUG("compiled fragment = %p (%s, %s)",
			data.call, fragment_ptr->name.c_str(),
			future->item->full_signature.c_str());

		ASSERT(data.call);

		// delete future
		auto& running_futures = context->block->running_futures;
		auto it = running_futures.find(future.get());
		ASSERT(it != running_futures.end() && "Future must exist");
		running_futures.erase(it);

		// propagate profiling
		auto& prof = future->jit_arg->profile;
		if (prof) {
			prof->compilation_time_us = fragment_ptr->compilation_time_us;
			prof->binary_size_bytes = fragment_ptr->binary_size_bytes;
		}


		// future = nullptr;
	}

	// LOG_TRACE("data.call = %p", data.call);
	VM_PRIM_PREFETCH_NEXT();
	templated_compiled_fragment<false, -1>(context, __ip, data);

#if 1
	// patch
	bool skipping = true;
	if (code_block->adaptive_actions) {
		item.call = VM_PRIM_GET(compiled_fragment_noprof);
		if (skipping && data.source_predicate_num) {
			item.call = VM_PRIM_GET(compiled_fragment_noprof_spn);
		}
	} else {
		item.call = VM_PRIM_GET(compiled_fragment_prof);
		if (skipping && data.source_predicate_num) {
			item.call = VM_PRIM_GET(compiled_fragment_prof_spn);
		}
	}
#endif
#endif
VM_PRIM_END(compiling_fragment)



// compiled fragment without profiling
VM_PRIM_BEGIN(compiled_fragment_noprof)
#ifdef VM_PRIM_IMPL
	VM_PRIM_PREFETCH_NEXT();

	auto& item = *context->item;
	auto& data = item.data.compiling_fragment;

	VM_PRIM_DEBUG("CALL [%llu] compiled_fragment_noprof with call %p",
		__ip, data.call);

	templated_compiled_fragment<false, -1>(context, __ip, data);
#endif
VM_PRIM_END(compiled_fragment_noprof)



// compiled fragment with profiling
VM_PRIM_BEGIN(compiled_fragment_prof)
#ifdef VM_PRIM_IMPL
	VM_PRIM_PREFETCH_NEXT();

	auto& item = *context->item;
	auto& data = item.data.compiling_fragment;

	VM_PRIM_DEBUG("CALL [%llu] compiled_fragment_prof with call %p",
		__ip, data.call);
	templated_compiled_fragment<true, -1>(context, __ip, data);
#endif
VM_PRIM_END(compiled_fragment_prof)


// compiled fragment without profiling with source_predicate_num
VM_PRIM_BEGIN(compiled_fragment_noprof_spn)
#ifdef VM_PRIM_IMPL
	VM_PRIM_PREFETCH_NEXT();

	auto& item = *context->item;
	auto& data = item.data.compiling_fragment;

	VM_PRIM_DEBUG("CALL [%llu] compiled_fragment_noprof_spn with call %p",
		__ip, data.call);
	templated_compiled_fragment<false, 1>(context, __ip, data);
#endif
VM_PRIM_END(compiled_fragment_noprof_spn)



// compiled fragment with profiling with source_predicate_num
VM_PRIM_BEGIN(compiled_fragment_prof_spn)
#ifdef VM_PRIM_IMPL
	VM_PRIM_PREFETCH_NEXT();

	auto& item = *context->item;
	auto& data = item.data.compiling_fragment;

	VM_PRIM_DEBUG("CALL [%llu] compiled_fragment_prof_spn with call %p",
		__ip, data.call);
	templated_compiled_fragment<true, 1>(context, __ip, data);
#endif
VM_PRIM_END(compiled_fragment_prof_spn)



VM_PRIM_BEGIN(seltrue)
#ifdef VM_PRIM_IMPL
	VM_PRIM_PREFETCH_NEXT();

	templated_seltruefalse<true, true>(context);
#endif
VM_PRIM_END(seltrue)


VM_PRIM_BEGIN(selfalse)
#ifdef VM_PRIM_IMPL
	VM_PRIM_PREFETCH_NEXT();

	templated_seltruefalse<false, true>(context);
#endif
VM_PRIM_END(selfalse)