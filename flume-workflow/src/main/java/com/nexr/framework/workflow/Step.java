package com.nexr.framework.workflow;

/**
 * @author dani.kim@nexr.com
 */
public class Step {
	private String name;
	private String next;
	private Class<? extends Tasklet> tasklet;
	
	public Step() {
	}
	
	public Step(String name, Class<? extends Tasklet> tasklet) {
		this.name = name;
		this.tasklet = tasklet;
	}
	
	public Step(String name, String next, Class<? extends Tasklet> tasklet) {
		this.name = name;
		this.next = next;
		this.tasklet = tasklet;
	}
	
	@Override
	public int hashCode() {
		if (name == null) {
			return super.hashCode();
		}
		return name.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (name == null || obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (obj instanceof Step) {
			if (name.equals(((Step) obj).getName())) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public String toString() {
		return new StringBuilder().append("step[name: ").append(name)
			.append(", next: ").append(next)
			.append(", tasklet: ").append(tasklet == null ? null : tasklet.getSimpleName())
			.append("]").toString();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getNext() {
		return next;
	}

	public void setNext(String next) {
		this.next = next;
	}

	public Class<? extends Tasklet> getTasklet() {
		return tasklet;
	}
	
	public void setTasklet(Class<? extends Tasklet> tasklet) {
		this.tasklet = tasklet;
	}
}
