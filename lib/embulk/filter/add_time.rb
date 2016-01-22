Embulk::JavaPlugin.register_filter(
  "add_time", "org.embulk.filter.add_time.AddTimeFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
