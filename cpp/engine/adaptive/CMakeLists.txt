add_library(engine_adaptive STATIC stream_reoptimizer.cpp brain.cpp decision.cpp decisions.cpp rule_triggers.cpp rule_triggers_impl.cpp metrics.cpp plan_profile.cpp)

target_link_libraries(engine_adaptive engine)
