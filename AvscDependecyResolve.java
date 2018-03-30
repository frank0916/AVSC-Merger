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
	char fp = File.separatorChar;

	String source, target;

	public AvscDependecyResolve(String source, String target) {
		this.source = source;
		this.target = target;
	}

	private Map<String, String> unsolved = new HashMap<>();

	/**
	 * Register a unsolved file
	 * 
	 * @param file
	 * @throws Exception
	 */
	public void register(File file) {
		try {
			String s = Files.lines(Paths.get(file.getPath()))
					.collect(Collectors.joining());
			int start = s.indexOf("{") + 1;
			int end = s.indexOf(",", start);
			String name = s.substring(start, end);
			name = name.split(":")[1].replace("\"", "").trim();
			unsolved.put(name, s);
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	private Map<String, String> resolved = new HashMap<>();

	Pattern p = Pattern.compile("\"[a-zA-Z][a-zA-Z0-9_]*\"");

	public String resolve(String key, String text) {
		String schema = resolved.get(key);
		if (schema != null) {
			return schema;
		}

		Matcher m = p.matcher(text);
		StringBuilder sb = new StringBuilder();
		int index = 0;
		while (m.find()) {
			int start = m.start();
			sb.append(text.substring(index, start));
			String token = m.group();
			String name = token.substring(1, token.length() - 1);
			String value = null;
			if (name.equals(key)) {
				value = token;
			} else {
				value = unsolved.get(name);
				value = value == null ? token : resolve(name, value);
			}
			sb.append(value);
			index = m.end();
		}
		sb.append(text.substring(index));
		schema = sb.toString();

		resolved.put(key, schema);
		return schema;
	}

	public void resolveDir(File dir) {
		File[] files = dir.listFiles();
		for (File file : files) {
			register(file);
		}
		String d = target + fp + dir.getName();
		if (!new File(d).mkdirs()) {
			System.err.println(d + " directory is not created.");
		}
		unsolved.forEach((k, v) -> {
			String s = resolve(k, v);
			File avsc = new File(d + fp + k + ".avsc");
			try (Writer w = new FileWriter(avsc)) {
				w.append(s);
				w.close();
			} catch (IOException e) {
				System.err.println("File write error : " + avsc);
			}
		});
	}

	public void resolveDirs() {
		try {
			File[] dirs = new File(source).listFiles(File::isDirectory);
			for (File dir : dirs) {
				resolveDir(dir);
			}
		} catch (Exception e) {
			System.err.println("Exception thrown in combineSchema " + e);
		}

	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Missing source or target files");
			System.err.println("Usage: java AvscDependecyResolve source targe");
			return;
		}
		new AvscDependecyResolve(args[0], args[1]).resolveDirs();
	}
}
