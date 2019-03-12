/*
 * HttpRunner provides method to create http connection
 */
package loadtester;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

/**
 *
 * @author frank
 */
public abstract class HttpRunner extends Runner {

	public abstract void parse(String response, Map<String, String> map);

	abstract Converter newConvertor();

	public Converter getConverter(int index) {
		Converter converter = Converter.getInstance(index);
		if (converter != null) {
			return converter;
		}
		synchronized (HttpRunner.class) {
			converter = Converter.getInstance(index);
			if (converter != null) {
				return converter;
			}
			return Converter.setInstance(index, newConvertor());
		}
	}

	HttpURLConnection open(String url, String method) throws Exception {
		try {
			URL u = new URL(url);
			HttpURLConnection c = (HttpURLConnection) u.openConnection();
			c.setRequestMethod(method);
			c.setDoOutput("POST".equals(method));
			c.setDoInput(true);
			return c;
		} catch (Throwable t) {
			Log.err("Fail to create connection " + method + " - " + url);
			return null;
		}
	}

	void run() {
		int ns = -1;
		Map<String, String> map = vars.get();
		int index = getIndex(map);
		Log.debug("index=" + index);
		try {
			HttpConverter converter = (HttpConverter) getConverter(index);
			String rqst = converter.get(2);
			rqst = Utils.replace(rqst, map);
			Log.debug("request=" + rqst);

			byte[] req = rqst.length() == 0 ? null : converter.encode(rqst);

			String s = converter.get(1);
			String url = Utils.replace(s, map);
			Log.debug("url=" + url);

			String method = req == null ? "GET" : "POST";

			Log.debug("method=" + method);

			long start = System.nanoTime();
			Log.debug("start=" + start);
			HttpURLConnection cn = open(url, method);
			converter.setRequestProperties(cn);
			cn.connect();

			if (req != null) {
				try (OutputStream os = cn.getOutputStream()) {
					os.write(req);
				}
			}
			int code = cn.getResponseCode();
			if (code >= 400) {
				try (InputStream in = cn.getErrorStream()) {
					Log.err("url : " + url);
					Log.err("request: " + rqst);
					String err = new String(in.readAllBytes());
					Log.err("error message : " + err);
				}
				ns = -2;
			} else {
				byte[] res = null;
				try (InputStream is = cn.getInputStream()) {
					res = is.readAllBytes();
					long end = System.nanoTime();
					Log.debug("end=" + end);
					String response = converter.decode(res);
					Log.debug("response=" + response);
					parse(response, map);
					ns = (int) ((end - start) / 10000);
				}
			}
			if (ns == -1 && index == 2) {
				Log.out("url : " + url);
				Log.out("request: " + rqst);
			}
		} catch (Throwable t) {
			ns = -3;
			Log.err("Unexpected error: " + index);
			Log.err(t);
		}

		PrintWriter p = getWriter();
		if (p != null) {
			p.printf("%d,%d\n", ns, index);
		}
	}
}
