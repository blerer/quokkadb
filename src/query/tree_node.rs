use std::rc::Rc;

pub trait TreeNode: Sized {
    type Child: TreeNode<Child = Self>; // Ensure that Self::Child is always Self

    /// Get child nodes as `Rc<Self>` references instead of moving them.
    fn children(&self) -> Vec<Rc<Self>>;

    /// Create a new node with updated children as `Rc<Self>`.
    fn with_new_children(self: Rc<Self>, children: Vec<Rc<Self>>) -> Rc<Self>;

    /// Recursive bottom-up transformation
    fn transform_up<F>(self: Rc<Self>, transform_fn: &F) -> Rc<Self>
    where
        F: Fn(Rc<Self>) -> Rc<Self>,
    {
        let mut children_changed = false;

        // Transform each child node
        let transformed_children: Vec<Rc<Self>> = self
            .children()
            .into_iter()
            .map(|child| {
                let transformed_child = child.clone().transform_up(transform_fn);
                if !Rc::ptr_eq(&transformed_child, &child) {
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
    fn transform_down<F>(self: Rc<Self>, transform_fn: &F) -> Rc<Self>
    where
        F: Fn(Rc<Self>) -> Rc<Self>,
    {
        let node = transform_fn(self.clone());

        let mut children_changed = false;

        // Transform each child node
        let transformed_children: Vec<Rc<Self>> = node
            .children()
            .into_iter()
            .map(|child| {
                let transformed_child = child.clone().transform_down(transform_fn);
                if !Rc::ptr_eq(&transformed_child, &child) {
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