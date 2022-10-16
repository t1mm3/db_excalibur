#pragma once

#include <unordered_map>
#include <vector>
#include <cassert>

namespace utils {

template<typename KeyType,
	typename ValueType,
	typename HashType = std::hash<KeyType>,
	typename EqualType = std::equal_to<KeyType>>
struct Cache {
private:
	struct Entry {
		std::pair<KeyType, ValueType> keyval;
		Entry* next = nullptr;
		Entry* prev = nullptr;

		Entry() {
		}

		Entry(std::pair<KeyType, ValueType>&& pair)
		 : keyval(std::move(pair)) {
		}
	};

	std::unordered_map<KeyType, Entry*, HashType, EqualType> map;
	Entry* head = nullptr;
	Entry* tail = nullptr;
	Entry* free = nullptr;
	std::vector<Entry> m_array;
	const size_t capacity;


	void init_free_list() {
		if (!m_array.empty()) {
			return;
		}
		m_array.resize(capacity);

		for (size_t i=0; i<m_array.size(); i++) {
			if (free) {
				m_array[i].next = free;
			}
			free = &m_array[i];
		}
	}

public:
	Cache(size_t cap) : capacity(cap) {
		
	}

	ValueType* lookup(KeyType key, bool update = true) {
		auto it = map.find(key);
		if (it == map.end()) {
			return nullptr;
		}
		auto entry = it->second;
		if (update) {
			move_to_head(entry);
		}

		return &entry->keyval.second;
	}

	ValueType* insert(std::pair<KeyType, ValueType>&& pair) {
		Entry* entry;

		auto it = map.find(pair.first);
		if (it != map.end()) {
			entry = it->second;

			remove(entry);
			free_entry(entry);
			entry = new_entry(std::move(pair));
			push_front(entry);

			it->second = entry;
		} else {
			// allocate new Entry
			entry = new_entry(std::move(pair));
			push_front(entry);

			map.insert({pair.first, entry});
		}

		return &entry->keyval.second;
	}

	bool erase(KeyType key) {
		auto it = map.find(key);
		if (it == map.end()) {
			return false;
		}
		remove(it->second);
		free_entry(it->second);
		map.erase(it);
		return true;
	}

	template<typename T>
	ValueType* lookup_or_create(const KeyType& key, const T& create) {
		auto r = lookup(key);
		if (r) {
			return r;
		}

		return insert({key, create()});
	}

	template<typename T>
	void for_each(const T& f)
	{
		Entry* curr = tail;
		while (curr) {
			f(curr->keyval);
			curr = curr->prev;
		}
	}

	template<typename T>
	void for_each(const T& f) const
	{
		Entry* curr = tail;
		while (curr) {
			f(curr->keyval);
			curr = curr->prev;
		}
	}

	size_t size() const {
		return map.size();
	}

	void clear() {
		map.clear();
		m_array.clear();
		m_array.resize(0);

		tail = nullptr;
		head = nullptr;
		free = nullptr;
	}

private:
	void move_to_head(Entry* entry) {
		remove(entry);
		push_front(entry);
	}

	void remove(Entry* entry) {
		if (entry == head) {
			head = entry->next;
			assert(!entry->prev);
		}
		if (entry == tail) {
			tail = entry->prev;
			assert(!entry->next);
		}

		if (entry->prev) {
			entry->prev->next = entry->next;
		}
		if (entry->next) {
			entry->next->prev = entry->prev;
		}

		entry->next = nullptr;
		entry->prev = nullptr;
	}

	void push_front(Entry* entry) {
		entry->prev = nullptr;
		entry->next = head;

		if (head) {
			head->prev = entry;
		}
		if (!tail) {
			tail = entry;
		}

		head = entry;
	}

	void free_entry(Entry* entry) {
		entry->~Entry();

		entry->next = nullptr;
		entry->prev = nullptr;

		entry->next = free;
		free = entry;
	}

	bool evict() {
		Entry* entry = tail;
		if (!entry) {
			return false;
		}

		remove(entry);

		map.erase(entry->keyval.first);
		free_entry(entry);
		return true;
	}

	Entry* new_entry(std::pair<KeyType, ValueType>&& pair) {
		if (!free) {
			if (m_array.empty()) {
				init_free_list();
			} else {
				evict();
			}
		}
		assert(free);
		auto entry = free; 
		free = free->next;

		return new (entry) Entry(std::move(pair));
	}
};

} /* utils */