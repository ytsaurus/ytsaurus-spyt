package tech.ytsaurus.spyt.patch;

import javassist.*;
import javassist.bytecode.ClassFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.spyt.SparkVersionUtils;
import tech.ytsaurus.spyt.patch.annotations.*;

import java.io.*;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;

/**
 * Strategies for patching Spark classes:
 *
 * <ol>
 * <li>Completely replace class bytecode with provided implementation. This is the default strategy. The patched
 * class implementation must be annotated only with {@link OriginClass} annotation that should be parameterized with
 * full name of the original class.</li>
 *
 * <li>Replace with subclass. In this strategy the original class is preserved but renamed to "original name"Base
 * at runtime. The patched class should be placed into the same package as the original class and annotated with
 * {@link Subclass} and {@link OriginClass} annotations. The latter should be parameterized with full name of the
 * original class. At runtime the patched class is renamed to "original name" and has "original name"Base superclass
 * which is actually the original class before patching</li>
 *
 * <li>Decorate methods. In this strategy the base class body is generally preserved but is enriched with additional
 * decorating methods which are defined in decorator class. The decorator class should be annotated with
 * {@link Decorate} and {@link OriginClass} annotations. The latter should be parameterized with full name of the
 * original class. In the decorating class the methods should also be annotated with {@link DecoratedMethod} annotation.
 * The method should has the same name and signature as the original method. Also there may be a stub method that
 * has the same signature and the same name that is prefixed with __ when it is required to call original method from
 * the decorating method.</li>
 * </ol>
 */
public class SparkPatchAgent {

    private static final Logger log = LoggerFactory.getLogger(SparkPatchAgent.class);

    public static void premain(String args, Instrumentation inst) {
        log.info("Starting SparkPatchAgent for hooking on jvm classloader");

        String classpath = System.getProperty("java.class.path");
        String[] classpathElements = classpath.split(File.pathSeparator);

        Stream<String> adapterImplPaths = Arrays.stream(classpathElements)
                .filter(path -> path.contains("spark-adapter-impl-") || path.contains("spark-adapter/impl"))
                .filter(path -> path.endsWith(".jar") || path.endsWith("/classes"));

        String patchJarPath = ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .filter(arg -> arg.startsWith("-javaagent") && arg.contains("spark-yt-spark-patch"))
                .map(arg -> arg.substring(arg.indexOf(':') + 1))
                .findFirst()
                .orElseThrow();

        Stream<String> patchSearchPaths = Stream.concat(Stream.of(patchJarPath), adapterImplPaths);

        Stream<String> classFiles = patchSearchPaths.flatMap(patchSearchPath -> {
            if (patchSearchPath.endsWith(".jar")) {
                return scanJarFile(patchSearchPath);
            } else {
                return scanClassesDirectory(patchSearchPath);
            }
        });

        Map<String,String> classMappings = classFiles
                .flatMap(fileName -> SparkPatchClassTransformer.toOriginClassName(fileName).stream())
                .collect(Collectors.toMap(s -> s[0], s -> s[1]));

        inst.addTransformer(new SparkPatchClassTransformer(classMappings));
    }

    private static Predicate<String> classFileFilter = fileName ->
            fileName.endsWith(".class") && !fileName.startsWith("tech/ytsaurus/");

    private static Stream<String> scanJarFile(String jarPath) {
        try(JarFile jarFile = new JarFile(jarPath)) {
            return jarFile.versionedStream()
                    .map(ZipEntry::getName)
                    .filter(classFileFilter)
                    .collect(Collectors.toList())
                    .stream();
            // Here we are collecting to list and then recreating a stream because we need to close jar file
            // inside this method.
        } catch (IOException e) {
            throw new SparkPatchException(e);
        }
    }

    private static Stream<String> scanClassesDirectory(String dirPath) {
        Path root = Path.of(dirPath);
        try (Stream<Path> paths = Files.walk(root)) {
            return paths
                    .map(path -> root.relativize(path).toString())
                    .filter(classFileFilter)
                    .collect(Collectors.toList())
                    .stream(); // See above on collecting and recreating a stream
        } catch (IOException e) {
            throw new SparkPatchException(e);
        }
    }

    private static String resourceToString(String name) throws IOException {
        try (var inputStream = SparkPatchAgent.class.getResourceAsStream(name)) {
            return inputStream == null ? null : new String(inputStream.readAllBytes());
        }
    }
}

class SparkPatchClassTransformer implements ClassFileTransformer {

    private static final Logger log = LoggerFactory.getLogger(SparkPatchAgent.class);
    private final Map<String, String> classMappings;
    private final Map<String, String> patchedClasses;

    static Optional<String[]> toOriginClassName(String fileName) {
        try {
            String patchClassName = fileName.substring(0, fileName.length() - 6);
            Optional<ClassFile> optClassFile = loadClassFile(fileName);
            if (optClassFile.isEmpty()) {
                return Optional.empty();
            }
            ClassFile classFile = optClassFile.get();
            CtClass ctClass = ClassPool.getDefault().makeClass(classFile);
            String originClass = getOriginClass(ctClass);
            boolean isApplicable = !ctClass.hasAnnotation(Applicability.class) ||
                    checkApplicability((Applicability) ctClass.getAnnotation(Applicability.class));
            PatchSource patchSource = (PatchSource) ctClass.getAnnotation(PatchSource.class);
            if (patchSource != null) {
                patchClassName = patchSource.value().replace('.', File.separatorChar);
            }
            if (originClass != null && isApplicable) {
                return Optional.of(new String[] {patchClassName, originClass.replace('.', File.separatorChar)});
            }
            return Optional.empty();
        } catch (ClassNotFoundException | IOException e) {
            throw new SparkPatchException(e);
        }
    }

    static String getOriginClass(CtClass ctClass) throws ClassNotFoundException {
        OriginClass originClassAnnotaion = (OriginClass) ctClass.getAnnotation(OriginClass.class);
        if (originClassAnnotaion != null) {
            String originClass = originClassAnnotaion.value();
            if (originClass.endsWith("$") && !ctClass.getName().endsWith("$")) {
                return null;
            }
            return originClass;
        }
        return null;
    }

    static ClassFile loadClassFile(byte[] classBytes) throws IOException {
        return new ClassFile(new DataInputStream(new ByteArrayInputStream(classBytes)));
    }
    static Optional<ClassFile> loadClassFile(String classFile) throws IOException {
        try (var inputStream = SparkPatchClassTransformer.class.getClassLoader().getResourceAsStream(classFile)) {
            return inputStream == null
                    ? Optional.empty()
                    : Optional.of(loadClassFile(inputStream.readAllBytes()));
        }
    }

    static byte[] serializeClass(ClassFile cf) throws IOException {
        ByteArrayOutputStream patchedBytesOutputStream = new ByteArrayOutputStream();
        cf.write(new DataOutputStream(patchedBytesOutputStream));
        patchedBytesOutputStream.flush();
        return patchedBytesOutputStream.toByteArray();
    }

    SparkPatchClassTransformer(Map<String, String> classMappings) {
        this.classMappings = classMappings;
        this.patchedClasses = classMappings
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        log.debug("Creating classfile transformer for the following classes: {}", patchedClasses);
    }

    @Override
    public byte[] transform(
            ClassLoader loader,
            String className,
            Class<?> classBeingRedefined,
            ProtectionDomain protectionDomain,
            byte[] classfileBuffer) {
        if (!patchedClasses.containsKey(className)) {
            return null;
        }

        try {
            ClassFile cf = loadClassFile(toPatchClassName(className)).orElseThrow();
            cf = processAnnotations(cf, loader, classfileBuffer);
            cf.renameClass(classMappings);
            byte[] patchedBytes = serializeClass(cf);

            log.info("Patch size for class {} is {} and after patching the size is {}",
                    className, classfileBuffer.length, patchedBytes.length);

            return patchedBytes;
        } catch (Exception e) {
            log.error(String.format("Can't patch class %s because an exception has occured", className), e);
            throw new SparkPatchException(e);
        }
    }

    private String toPatchClassName(String className) {
        return patchedClasses.get(className) + ".class";
    }

    private ClassFile processAnnotations(ClassFile cf, ClassLoader loader, byte[] baseClassBytes) throws Exception {
        CtClass ctClass = ClassPool.getDefault().makeClass(cf);
        String originClass = getOriginClass(ctClass);
        if (originClass == null) {
            return cf;
        }

        ClassFile processedClassFile = cf;

        for (Object annotation : ctClass.getAnnotations()) {
            if (annotation instanceof Subclass) {
                String baseClass = originClass + "Base";
                log.info("Changing superclass of {} to {}", originClass, baseClass);
                cf.setSuperclass(baseClass);
                cf.renameClass(originClass, baseClass);

                ClassFile baseCf = loadClassFile(baseClassBytes);
                baseCf.renameClass(originClass, baseClass);
                ClassPool.getDefault().makeClass(baseCf).toClass(loader, null);
            }

            if (annotation instanceof AddInterfaces) {
                ClassFile baseCf = loadClassFile(baseClassBytes);
                for (Class<?> i : ((AddInterfaces) annotation).value()) {
                    baseCf.addInterface(i.getName());
                }
                CtClass baseCtClass = ClassPool.getDefault().makeClass(baseCf);
                processedClassFile = baseCtClass.getClassFile();
            }

            if (annotation instanceof Decorate) {
                ClassFile baseCf = loadClassFile(baseClassBytes);
                CtClass baseCtClass = ClassPool.getDefault().makeClass(baseCf);
                for (CtMethod method : ctClass.getDeclaredMethods()) {
                    if (checkDecoratedMethod(method)) {
                        DecoratedMethod dmAnnotation = (DecoratedMethod) method.getAnnotation(DecoratedMethod.class);
                        String methodName = method.getName();
                        CtMethod baseMethod = baseCtClass.getMethod(methodName, method.getSignature());
                        log.debug("Patching decorated method {} with signature {}",
                                methodName,
                                baseMethod.getSignature()
                        );
                        String innerMethodName = "__" + methodName;
                        baseMethod.setName(innerMethodName);

                        for (Class<? extends MethodProcesor> methodProcessorClass : dmAnnotation.baseMethodProcessors()) {
                            MethodProcesor procesor = methodProcessorClass.getDeclaredConstructor().newInstance();
                            procesor.process(baseMethod);
                        }

                        CtMethod newMethod = CtNewMethod.copy(method, baseCtClass, null);
                        baseCtClass.addMethod(newMethod);
                    }
                }

                processedClassFile = baseCtClass.getClassFile();
            }
        }

        return processedClassFile;
    }

    private static boolean checkDecoratedMethod(CtMethod method) throws Exception {
        return method.hasAnnotation(DecoratedMethod.class) &&
                (!method.hasAnnotation(Applicability.class) ||
                        checkApplicability((Applicability) method.getAnnotation(Applicability.class)));
    }

    private static boolean checkApplicability(Applicability applicability) {
        var ordering = SparkVersionUtils.ordering();
        String currentVersion = SparkVersionUtils.currentVersion();

        return (applicability.from().isEmpty() || ordering.gteq(currentVersion, applicability.from())) &&
                (applicability.to().isEmpty() || ordering.lteq(currentVersion, applicability.to()));
    }
}