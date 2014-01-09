package com.mongodb.resharder;

import static spark.Spark.setPort;
import static spark.Spark.get;
import static spark.Spark.post;
import spark.Request;
import spark.Response;
import spark.Route;

import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import freemarker.template.Configuration;
import freemarker.template.SimpleHash;
import freemarker.template.Template;
import freemarker.template.TemplateException;

public class Launcher {
	private final Configuration _cfg;
	public static ScheduledExecutorService _tp = Executors.newScheduledThreadPool(25);
	private long _ts = 0;

	public static void main(String[] args) throws IOException {
		Config.processArgs(args);
		System.setErr(new PrintStream("./err.log"));

		if (Config.isCLI()) {
			Shell.runCLI();
		} else {
			new Launcher();
		}
	}

	public Launcher() throws IOException {

		try {
			_cfg = createFreemarkerConfiguration();
			setPort(8082);
			initializeRoutes();
		} finally {
		}
	}

	abstract class FreemarkerBasedRoute extends Route {
		final Template template;

		protected FreemarkerBasedRoute(final String path, final String templateName) throws IOException {
			super(path);
			template = _cfg.getTemplate(templateName);
		}

		@Override
		public Object handle(Request request, Response response) {
			StringWriter writer = new StringWriter();
			try {
				doHandle(request, response, writer);
			} catch (Exception e) {
				e.printStackTrace();
				response.redirect("/internal_error");
			}
			return writer;
		}

		protected abstract void doHandle(final Request request, final Response response, final Writer writer)
				throws IOException, TemplateException;

	}

	private void initializeRoutes() throws IOException {
		get(new FreemarkerBasedRoute("/", "index.ftl") {
			@Override
			public void doHandle(Request request, Response response, Writer writer) throws IOException,
					TemplateException {
				SimpleHash root = new SimpleHash();

				template.process(root, writer);
			}
		});

		get(new FreemarkerBasedRoute("/reshard", "reshard.ftl") {
			@Override
			public void doHandle(Request request, Response response, Writer writer) throws IOException,
					TemplateException {

				if (!DocWriter.get_running()) {
					try {
						Map<String, String> map = new HashMap<String,String>();
						
						map.put("namespace", request.queryParams("namespace"));
						map.put("targetns", request.queryParams("targetns"));
						map.put("readBatch", request.queryParams("readBatch"));
						map.put("writeBatch", request.queryParams("writeBatch"));
						map.put("reshard", request.queryParams("reshard"));
						map.put("key", request.queryParams("key"));
						map.put("secondary", request.queryParams("secondary"));
						map.put("srchost", request.queryParams("srchost"));
						map.put("tgthost", request.queryParams("tgthost"));
						map.put("loghost", request.queryParams("loghost"));
						
						Config.init(map);

						_tp.execute(new Resharder());
					} catch (NumberFormatException e) {
						e.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				template.process(new SimpleHash(), writer);
			};
		});

		get(new FreemarkerBasedRoute("/getCounters", "counters.ftl") {
			@Override
			protected void doHandle(Request request, Response response, Writer writer) throws IOException,
					TemplateException {
				SimpleHash hash = new SimpleHash();
				Map<String, Long> map = Config.getCounters();
				hash.put("docCount", map.get("docCount"));
				hash.put("orphanCount", map.get("orphanCount"));
				hash.put("oplogCount", map.get("oplogCount"));

				if (_ts > 0) {
					long secs = ((System.currentTimeMillis() - _ts) / 1000);
					if (secs == 0) {
						secs = 1;
					}

					if (DocWriter.get_running()) {
						hash.put("docsPerSec", map.get("docCount") / secs);
						hash.put("orphansPerSec", map.get("orphanCount") / secs);
					} else {
						hash.put("docsPerSec", 0);
						hash.put("orphansPerSec", 0);
					}

					hash.put("oplogsPerSec", map.get("oplogCount") / secs);

				} else {
					hash.put("docsPerSec", 0);
					hash.put("orphansPerSec", 0);
					hash.put("oplogsPerSec", 0);

					_ts = System.currentTimeMillis();
				}

				template.process(hash, writer);

			}

		});

		get(new FreemarkerBasedRoute("/getChartData", "countervals.ftl") {
			@Override
			protected void doHandle(Request request, Response response, Writer writer) throws IOException,
					TemplateException {
				SimpleHash hash = new SimpleHash();

				hash.put("data", PerfCounters.getRateCounters());

				template.process(hash, writer);
			}

		});

		get(new FreemarkerBasedRoute("/form.js", "form.js") {
			@Override
			protected void doHandle(Request request, Response response, Writer writer) throws IOException,
					TemplateException {
				template.process(new SimpleHash(), writer);

			}

		});

		get(new FreemarkerBasedRoute("/jquery-2.0.3.min.js", "jquery-2.0.3.min.js") {
			@Override
			protected void doHandle(Request request, Response response, Writer writer) throws IOException,
					TemplateException {
				template.process(new SimpleHash(), writer);

			}

		});

		get(new FreemarkerBasedRoute("getStatus", "status.ftl") {
			@Override
			protected void doHandle(Request request, Response response, Writer writer) throws IOException,
					TemplateException {
				SimpleHash hash = new SimpleHash();
				hash.put("messages", MessageLog.getRecentMessages());
				template.process(hash, writer);

			}

		});

		get(new FreemarkerBasedRoute("isActive", "result.ftl") {
			@Override
			protected void doHandle(Request request, Response response, Writer writer) throws IOException,
					TemplateException {
				SimpleHash hash = new SimpleHash();
				hash.put("result", OpLogWriter.isActive() ? "true" : "false");
				template.process(hash, writer);
			}
		});

		post(new FreemarkerBasedRoute("/end", "result.ftl") {
			@Override
			public void doHandle(Request request, Response response, Writer writer) throws IOException,
					TemplateException {
				SimpleHash hash = new SimpleHash();
				hash.put("result", "true");

				try {
					Resharder.shutdown();
				} catch (Exception e) {
					MessageLog.push("ERROR: " + e.getMessage(), this.getClass().getSimpleName());
					e.printStackTrace();
					hash.put("result", "false");
				}

				template.process(hash, writer);
			}
		});

		post(new FreemarkerBasedRoute("/start", "start.ftl") {
			@Override
			public void doHandle(Request request, Response response, Writer writer) throws IOException,
					TemplateException {
				SimpleHash hash = new SimpleHash();

				// set initial perf counter values
				hash.put("data", PerfCounters.getRateCounters());

				_ts = 0;

				OpLogReader.shutdown();

				template.process(ShardMapper.getShardingStatus(hash), writer);
			}
		});

	}

	private Configuration createFreemarkerConfiguration() {
		Configuration retVal = new Configuration();
		retVal.setClassForTemplateLoading(Launcher.class, "/freemarker");
		return retVal;
	}

}
