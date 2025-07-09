use std::sync::Arc;

pub trait TreeNode: Sized {
    type Child: TreeNode<Child = Self>; // Ensure that Self::Child is always Self

    /// Get child nodes as `Rc<Self>` references instead of moving them.
    fn children(&self) -> Vec<Arc<Self>>;

    /// Create a new node with updated children as `Rc<Self>`.
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<Self>>) -> Arc<Self>;

    /// Recursive bottom-up transformation
    fn transform_up<F>(self: Arc<Self>, transform_fn: &F) -> Arc<Self>
    where
        F: Fn(Arc<Self>) -> Arc<Self>,
    {
        let mut children_changed = false;

        // Transform each child node
        let transformed_children: Vec<Arc<Self>> = self
            .children()
            .into_iter()
            .map(|child| {
                let transformed_child = child.clone().transform_up(transform_fn);
                if !Arc::ptr_eq(&transformed_child, &child) {
                    children_changed = true;
                }
                transformed_child
            })
            .collect();

        // If no children changed, return the existing Rc<Self> reference
        if !children_changed {
            return transform_fn(self);
        }

        // Rebuild the node only if any child changed
        let updated_self = self.with_new_children(transformed_children);

        // Apply transformation function
        transform_fn(updated_self)
    }

    /// Recursive top-down transformation
    fn transform_down<F>(self: Arc<Self>, transform_fn: &F) -> Arc<Self>
    where
        F: Fn(Arc<Self>) -> Arc<Self>,
    {
        let node = transform_fn(self.clone());

        let mut children_changed = false;

        // Transform each child node
        let transformed_children: Vec<Arc<Self>> = node
            .children()
            .into_iter()
            .map(|child| {
                let transformed_child = child.clone().transform_down(transform_fn);
                if !Arc::ptr_eq(&transformed_child, &child) {
                    children_changed = true;
                }
                transformed_child
            })
            .collect();

        // If no children changed, return the already transformed node
        if !children_changed {
            return node;
        }

        // Rebuild the node as the children have changed
        node.with_new_children(transformed_children)
    }
}
