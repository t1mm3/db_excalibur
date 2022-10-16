#pragma once

#include <memory>
struct Event {
	virtual void log_message() = 0;
};

struct EventListener {
	virtual void event(const Event& event) = 0;
};

struct EventBus {
	void add(std::unique_ptr<Event>&& event);

	void listen(const std::shared_ptr<EventListener>& listener);
};

extern EventBus g_event_bus;