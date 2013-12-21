package com.mongodb.resharder;

import static spark.Spark.setPort;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.setPort;
import spark.Request;
import spark.Response;
import spark.Route;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.GroupCommand;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import freemarker.template.Configuration;
import freemarker.template.SimpleHash;
import freemarker.template.Template;
import freemarker.template.TemplateException;

public class Launcher {
	private final Configuration _cfg;

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
				SimpleHash root = new SimpleHash();

				//TODO - prevent configuration from changing while cloning is in progress
				Config.set_Namespace(request.queryParams("namespace"));
				Config.set_TargetNamepace(request.queryParams("targetns"));
				Config.set_readBatch(Integer.parseInt(request.queryParams("readBatch")));
				Config.set_writeBatch(Integer.parseInt(request.queryParams("writeBatch")));
				
				Resharder rs = new Resharder();
				new Thread(rs).start();
				
				template.process(root, writer);
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
