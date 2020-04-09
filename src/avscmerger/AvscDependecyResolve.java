package avscmerger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class AvscDependecyResolve {
	Pattern pattern = Pattern.compile("\"[a-zA-Z][a-zA-Z0-9_.]*[a-zA-Z0-9]\"");
	char FS = File.separatorChar;

	String source, target;

	public AvscDependecyResolve(String source, String target) {
		this.source = source;
		this.target = target;
	}

	private Map<String, String> registry = new HashMap<>();
	private Map<String, String> resolved = new HashMap<>();

	String getId(String text) {
		int start = text.indexOf("{") + 1;
		int end = text.indexOf(",", start);
		String id = text.substring(start, end);
		id = id.split(":")[1].replace("\"", "").trim();
		start = text.indexOf("namespace", end);
		end = text.indexOf(",", start);
		String namespace = text.substring(start, end);
		namespace = namespace.split(":")[1].replace("\"", "").trim();
		id = namespace + "." + id;
		return id;
	}
	
	
	/**
	 * Register a file
	 * 
	 * @param file
	 * @throws IOException
	 * @throws Exception
	 */
	void register(File f) throws IOException {
		if (f.isFile()) {
			String contents = Files.lines(Paths.get(f.getPath()))
					.collect(Collectors.joining());
			String id = getId(contents);
			registry.put(id, contents);
		} else if (f.isDirectory()) {
			File[] files = f.listFiles();
			for (File file : files) {
				register(file);
			}
		}
	}

	/**
	 * @throws IOException
	 */
	void register() throws IOException {
		File f = new File(source);
		register(f);
	}

	/**
	 * @param id
	 * @param text
	 * @return
	 */
	public String resolve(String id) {
		String schema = resolved.get(id);
		if (schema != null) {
			return schema;
		}

		int index = id.lastIndexOf('.');
		String shortId = index < 0 ? id : id.substring(index + 1);
		String namespace = index < 0 ? null : id.substring(0, index);

		String text = registry.get(id);
		Matcher matcher = pattern.matcher(text);
		StringBuilder sb = new StringBuilder();
		index = 0;
		while (matcher.find()) {
			int start = matcher.start();
			sb.append(text.substring(index, start));
			String token = matcher.group();
			String key = token.substring(1, token.length() - 1);
			String value = null;

			if (key.equals(shortId)) {
				// ignore the first '"name", "${id}" section
				value = token;
			} else {
				if (key.indexOf('.') >= 0) {
					value = registry.get(key);
				} else {
					value = registry.get(namespace + '.' + key);
				}
				value = value == null ? token : resolve(getId(value));
			}

			sb.append(value);
			index = matcher.end();
		}
		sb.append(text.substring(index));
		schema = sb.toString();

		resolved.put(id, schema);
		return schema;
	}

	/**
	 * 
	 */
	private void resolve() {
		registry.keySet().forEach((k) -> {
			String s = resolve(k);
			File path = new File(target + FS + k + ".avsc");
			try (Writer w = new FileWriter(path)) {
				w.append(s);
				w.close();
			} catch (IOException e) {
				System.err.println("File write error : " + path);
			}
		});
	}

	/**
	 * @param args
	 */

	/*
	 * [0] is source directory of AVSC files only
	 * 
	 * [1] is target directory to store converted AVSC files
	 * 
	 * all files in source directory must be in AVSC format
	 * 
	 * target directory should not exist or remove before starting
	 * 
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: java AvscDependecyResolve source targe");
			System.err.println("source directory of AVSC files to merge.");
			System.err.println("target directory wiil have combined files.");
			return;
		}
		AvscDependecyResolve r = new AvscDependecyResolve(args[0], args[1]);
		File f = new File(r.target);
		if (!f.mkdirs()) {
			System.err.print(r.target);
			var msg = " - failed to create directory. stopped.";
			System.err.println(msg);
			System.exit(1);
		}

		try {
			r.register();
			r.resolve();
		} catch (IOException e) {
			var msg = "Fail to create combined schema file(s).";
			System.err.println(msg);
			e.printStackTrace();
		}
	}
}

