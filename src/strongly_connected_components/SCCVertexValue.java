package strongly_connected_components;

public class SCCVertexValue {
	private long id;
	private long color;
	private boolean isFinal;
	private long inDegree;
	private long outDegree;
	private boolean firstIteration;
	private boolean active;
	public int numIter;

	private boolean colorRootReachable;
	private boolean isColorRoot;
	
	public SCCVertexValue(long id) {
		setId(id);
		setColor(Long.MAX_VALUE);
		setFinal(false);
		setInDegree(-1);
		setOutDegree(-1);
		setFirstIteration(true);
		setActive(true);
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public long getColor() {
		return color;
	}

	public void setColor(long color) {
		this.color = color;
	}

	public boolean isFinal() {
		return isFinal;
	}

	public void setFinal(boolean isFinal) {
		this.isFinal = isFinal;
	}

	public long getInDegree() {
		return inDegree;
	}

	public void setInDegree(long inDegree) {
		this.inDegree = inDegree;
	}

	public long getOutDegree() {
		return outDegree;
	}

	public void setOutDegree(long outDegree) {
		this.outDegree = outDegree;
	}

	public boolean isFirstIteration() {
		return firstIteration;
	}

	public void setFirstIteration(boolean firstIteration) {
		this.firstIteration = firstIteration;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}
	
	public boolean vertexIsFinal() {
		return inDegree <= 0 || outDegree <= 0;
	}
	
	@Override
	public String toString() {
		return String.format("id: %s, color: %s, isFinal: %s, inDegree: %s, outDegree: %s, active: %s, isColorRoot: %s, colorRootReachable: %s, numIter: %s",
				id, color, isFinal, inDegree, outDegree, active, isColorRoot, colorRootReachable, numIter);
	}

    public boolean isColorRoot() {
        return isColorRoot;
    }

    public void setColorRoot(boolean colorRoot) {
        isColorRoot = colorRoot;
    }

    public boolean isColorRootReachable() {
        return colorRootReachable;
    }

    public void setColorRootReachable(boolean existsSameColorFinalNeighbor) {
        this.colorRootReachable = existsSameColorFinalNeighbor;
    }
}
