plugins {
    java
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.hsqldb:hsqldb:2.6.0")
    implementation("org.apache.commons:commons-csv:1.9.0")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.7.2")
}

val fatJar = task("fatJar", type = Jar::class) {
    archiveBaseName.set("${project.name}-fat")
    manifest {
        attributes["Implementation-Title"] = "Fat Jar"
        attributes["Implementation-Version"] = 1.0
        attributes["Main-Class"] = "ru.hse.task.Main"
    }
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    with(tasks.jar.get() as CopySpec)
}

tasks {
    "build" {
        dependsOn(fatJar)
    }
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}