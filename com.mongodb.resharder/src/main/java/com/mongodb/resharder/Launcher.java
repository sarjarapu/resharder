package com.mongodb.resharder;

import static spark.Spark.setPort;
import static spark.Spark.get;
import static spark.Spark.post;
import spark.Request;
import spark.Response;
import spark.Route;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import freemarker.template.Configuration;
import freemarker.template.SimpleHash;
import freemarker.template.Template;
import freemarker.template.TemplateException;

public class Launcher {
	private final Configuration _cfg;
	public static ScheduledExecutorService _tp = Executors.newScheduledThreadPool(25);

	public static void main(String[] args) throws IOException {
		Config.processArgs(args);

		new Launcher();
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
						new Config(request.queryParams("namespace"), request.queryParams("targetns"),
								Integer.parseInt(request.queryParams("readBatch")), Integer.parseInt(request.queryParams("writeBatch")),
								Boolean.parseBoolean(request.queryParams("reshard")), request.queryParams("key"),
								Boolean.parseBoolean(request.queryParams("secondary")), request.queryParams("srchost"),
								request.queryParams("tgthost"), request.queryParams("loghost"));

						Resharder rs = new Resharder();
						_tp.schedule(rs, 0, TimeUnit.MILLISECONDS);
					} catch (NumberFormatException e) {
						e.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				template.process(new SimpleHash(), writer);
			};
		});

		get(new FreemarkerBasedRoute("/styles.css", "styles.css") {
			@Override
			protected void doHandle(Request request, Response response, Writer writer) throws IOException,
					TemplateException {
				template.process(new SimpleHash(), writer);

			}

		});

		get(new FreemarkerBasedRoute("/form.js", "form.js") {
			@Override
			protected void doHandle(Request request, Response response, Writer writer) throws IOException,
					TemplateException {
				template.process(new SimpleHash(), writer);

			}

		});

		get(new FreemarkerBasedRoute("/jquery.sparkline.min.js", "jquery.sparkline.min.js") {
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

		post(new FreemarkerBasedRoute("/start", "start.ftl") {
			@Override
			public void doHandle(Request request, Response response, Writer writer) throws IOException,
					TemplateException {
				SimpleHash root = new SimpleHash();

				template.process(ShardMapper.getShardingStatus(root), writer);
			}
		});

	}

	private Configuration createFreemarkerConfiguration() {
		Configuration retVal = new Configuration();
		retVal.setClassForTemplateLoading(Launcher.class, "/freemarker");
		return retVal;
	}

}
